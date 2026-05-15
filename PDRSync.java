/**
 * ============================================================
 *  PDR-Sync — Precision Disaster Recovery Coordination System
 *  Software Construction Course Project
 * ============================================================
 *  Single-file Java application:
 *    - Java Swing frontend (sidebar navigation, card dashboard)
 *    - Embedded HTTP server (com.sun.net.httpserver)
 *    - MySQL backend via JDBC
 *    - Normalization Engine (Strategy Pattern)
 *    - Confidence Decay Engine
 *    - Command Queue (offline-first sync)
 *    - Mesh Node Registry (simulated LAN P2P)
 * ============================================================
 *  Author : [Your Name]
 *  Course : Software Construction
 *  Date   : 2025
 * ============================================================
 */

import com.sun.net.httpserver.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.awt.geom.*;
import java.io.*;
import java.net.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.UUID;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class PDRSync {

    // ─────────────────────────────────────────────────────────
    //  1. CONFIGURATION
    // ─────────────────────────────────────────────────────────
    static final String DB_URL  = "jdbc:mysql://localhost:3306/pdrsync?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    static final String DB_USER = "root";
    static final String DB_PASS = "rosh@44"; // ← change this
    static final int    API_PORT = 8080;

    static final long DECAY_FULL_MS = 4 * 60 * 60 * 1000L;
    static final long DECAY_WARN_MS = 2 * 60 * 60 * 1000L;

    // ─────────────────────────────────────────────────────────
    //  2. NORMALIZATION ENGINE (Strategy Pattern)
    // ─────────────────────────────────────────────────────────
    interface NormalizationStrategy {
        double toBaseUnit(double amount);
        String baseUnitLabel();
        String[] handledUnits();
    }

    static class LiquidStrategy implements NormalizationStrategy {
        public double toBaseUnit(double a) { return a; }
        public String baseUnitLabel() { return "liters"; }
        public String[] handledUnits() { return new String[]{"liters","l","gallons","gal","ml"}; }
    }

    static class WeightStrategy implements NormalizationStrategy {
        public double toBaseUnit(double a) { return a; }
        public String baseUnitLabel() { return "kg"; }
        public String[] handledUnits() { return new String[]{"kg","kilograms","lbs","pounds","tons","pallets"}; }
    }

    static class CountStrategy implements NormalizationStrategy {
        public double toBaseUnit(double a) { return a; }
        public String baseUnitLabel() { return "units"; }
        public String[] handledUnits() { return new String[]{"units","pieces","cases","boxes","packs","items"}; }
    }

    static class NormalizationEngine {
        private static final Map<String, NormalizationStrategy> REGISTRY = new HashMap<>();
        private static final Map<String, Double> CONV = new HashMap<>();

        static {
            for (NormalizationStrategy s : new NormalizationStrategy[]{
                    new LiquidStrategy(), new WeightStrategy(), new CountStrategy()}) {
                for (String u : s.handledUnits()) REGISTRY.put(u.toLowerCase(), s);
            }
            CONV.put("gallons", 3.785); CONV.put("gal", 3.785);
            CONV.put("ml", 0.001); CONV.put("liters", 1.0); CONV.put("l", 1.0);
            CONV.put("kg", 1.0); CONV.put("kilograms", 1.0);
            CONV.put("lbs", 0.453); CONV.put("pounds", 0.453);
            CONV.put("tons", 1000.0); CONV.put("pallets", 500.0);
            CONV.put("units", 1.0); CONV.put("pieces", 1.0);
            CONV.put("cases", 12.0); CONV.put("boxes", 6.0);
            CONV.put("packs", 24.0); CONV.put("items", 1.0);
        }

        static NormalizedResult normalize(double amount, String unit) {
            String key = unit.trim().toLowerCase();
            NormalizationStrategy strategy = REGISTRY.getOrDefault(key, new CountStrategy());
            double factor = CONV.getOrDefault(key, 1.0);
            double base   = amount * factor;
            return new NormalizedResult(base, strategy.baseUnitLabel(), strategy.getClass().getSimpleName());
        }
    }

    record NormalizedResult(double baseAmount, String baseUnit, String strategyUsed) {
        public String display() { return String.format("%.2f %s (via %s)", baseAmount, baseUnit, strategyUsed); }
    }

    // ─────────────────────────────────────────────────────────
    //  3. CONFIDENCE DECAY ENGINE
    // ─────────────────────────────────────────────────────────
    static class DecayEngine {
        static int computeConfidence(long reportedAtMs) {
            long age = System.currentTimeMillis() - reportedAtMs;
            if (age <= 0) return 100;
            if (age >= DECAY_FULL_MS) return 0;
            return (int)(100.0 * (1.0 - (double) age / DECAY_FULL_MS));
        }

        static Color confidenceColor(int pct) {
            if (pct >= 70) return C_FRESH;
            if (pct >= 40) return C_AGING;
            return C_STALE;
        }

        static Color confidenceBg(int pct) {
            if (pct >= 70) return new Color(220, 252, 231);
            if (pct >= 40) return new Color(254, 243, 199);
            return new Color(254, 226, 226);
        }

        static String confidenceLabel(int pct) {
            if (pct >= 70) return "Fresh";
            if (pct >= 40) return "Aging";
            return "Stale";
        }
    }

    // ─────────────────────────────────────────────────────────
    //  4. ACTION OBJECT (Command Queue entry)
    // ─────────────────────────────────────────────────────────
    static class ActionObject {
        final String id;
        final String type;
        final Map<String, String> payload;
        final long timestamp;
        boolean synced;

        ActionObject(String type, Map<String, String> payload) {
            this.id        = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            this.type      = type;
            this.payload   = payload;
            this.timestamp = System.currentTimeMillis();
            this.synced    = false;
        }
    }

    // ─────────────────────────────────────────────────────────
    //  5. COMMAND QUEUE
    // ─────────────────────────────────────────────────────────
    static class CommandQueue {
        private final Deque<ActionObject> queue = new ArrayDeque<>();
        private final AtomicBoolean online;

        CommandQueue(AtomicBoolean online) { this.online = online; }

        synchronized void enqueue(ActionObject a) {
            queue.addLast(a);
            System.out.println("[Queue] Enqueued: " + a.type + " #" + a.id);
            if (online.get()) flush();
        }

        synchronized List<ActionObject> snapshot() { return new ArrayList<>(queue); }
        synchronized int pending() { return (int) queue.stream().filter(a -> !a.synced).count(); }

        synchronized void flush() {
            for (ActionObject a : queue) {
                if (!a.synced) {
                    boolean ok = Database.persistAction(a);
                    if (ok) { a.synced = true; System.out.println("[Queue] Synced: " + a.id); }
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────
    //  6. DATABASE LAYER
    // ─────────────────────────────────────────────────────────
    static class Database {
        private static Connection conn;

        static boolean connect() {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                initSchema();
                System.out.println("[DB] Connected to MySQL");
                return true;
            } catch (Exception e) {
                System.err.println("[DB] Connection failed: " + e.getMessage());
                return false;
            }
        }

        static void initSchema() throws SQLException {
            try (Statement s = conn.createStatement()) {
                s.execute("""
                    CREATE TABLE IF NOT EXISTS resources (
                        id           INT AUTO_INCREMENT PRIMARY KEY,
                        name         VARCHAR(200) NOT NULL,
                        category     VARCHAR(100),
                        amount_raw   DOUBLE,
                        unit_raw     VARCHAR(50),
                        amount_base  DOUBLE,
                        unit_base    VARCHAR(50),
                        strategy     VARCHAR(100),
                        location     VARCHAR(200),
                        reporter     VARCHAR(100),
                        reported_at  BIGINT,
                        confidence   INT,
                        status       VARCHAR(50) DEFAULT 'ACTIVE'
                    )
                """);
                s.execute("""
                    CREATE TABLE IF NOT EXISTS action_queue (
                        id          VARCHAR(36) PRIMARY KEY,
                        type        VARCHAR(100),
                        payload     TEXT,
                        queued_at   BIGINT,
                        synced      BOOLEAN DEFAULT FALSE
                    )
                """);
                s.execute("""
                    CREATE TABLE IF NOT EXISTS mesh_nodes (
                        node_id     VARCHAR(100) PRIMARY KEY,
                        ip_address  VARCHAR(50),
                        last_seen   BIGINT,
                        relay_count INT DEFAULT 0,
                        status      VARCHAR(50) DEFAULT 'ONLINE'
                    )
                """);
            }
        }

        static boolean persistAction(ActionObject a) {
            if (conn == null) return false;
            try {
                PreparedStatement ps = conn.prepareStatement(
                    "INSERT IGNORE INTO action_queue(id,type,payload,queued_at,synced) VALUES(?,?,?,?,?)");
                ps.setString(1, a.id); ps.setString(2, a.type);
                ps.setString(3, a.payload.toString()); ps.setLong(4, a.timestamp); ps.setBoolean(5, true);
                ps.executeUpdate();

                if ("REPORT_RESOURCE".equals(a.type)) {
                    Map<String, String> p = a.payload;
                    double rawAmt = Double.parseDouble(p.getOrDefault("amount", "0"));
                    String rawUnit = p.getOrDefault("unit", "units");
                    NormalizedResult nr = NormalizationEngine.normalize(rawAmt, rawUnit);
                    long ts = a.timestamp;
                    int conf = DecayEngine.computeConfidence(ts);
                    PreparedStatement ins = conn.prepareStatement(
                        "INSERT INTO resources(name,category,amount_raw,unit_raw,amount_base,unit_base,strategy,location,reporter,reported_at,confidence) VALUES(?,?,?,?,?,?,?,?,?,?,?)");
                    ins.setString(1, p.getOrDefault("name","Unknown"));
                    ins.setString(2, p.getOrDefault("category","General"));
                    ins.setDouble(3, rawAmt); ins.setString(4, rawUnit);
                    ins.setDouble(5, nr.baseAmount()); ins.setString(6, nr.baseUnit());
                    ins.setString(7, nr.strategyUsed());
                    ins.setString(8, p.getOrDefault("location","Unknown"));
                    ins.setString(9, p.getOrDefault("reporter","Anonymous"));
                    ins.setLong(10, ts); ins.setInt(11, conf);
                    ins.executeUpdate();
                }
                return true;
            } catch (SQLException e) {
                System.err.println("[DB] Persist failed: " + e.getMessage());
                return false;
            }
        }

        static List<Map<String, Object>> fetchResources() {
            List<Map<String, Object>> rows = new ArrayList<>();
            if (conn == null) return rows;
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM resources ORDER BY reported_at DESC")) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id",         rs.getInt("id"));
                    row.put("name",       rs.getString("name"));
                    row.put("category",   rs.getString("category"));
                    row.put("amount_raw", rs.getDouble("amount_raw") + " " + rs.getString("unit_raw"));
                    row.put("normalized", String.format("%.2f %s", rs.getDouble("amount_base"), rs.getString("unit_base")));
                    row.put("location",   rs.getString("location"));
                    row.put("reporter",   rs.getString("reporter"));
                    row.put("reported_at",rs.getLong("reported_at"));
                    row.put("confidence", DecayEngine.computeConfidence(rs.getLong("reported_at")));
                    rows.add(row);
                }
            } catch (SQLException e) { System.err.println("[DB] Fetch failed: " + e.getMessage()); }
            return rows;
        }

        static void registerNode(String nodeId, String ip) {
            if (conn == null) return;
            try {
                PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO mesh_nodes(node_id,ip_address,last_seen,relay_count) VALUES(?,?,?,1) " +
                    "ON DUPLICATE KEY UPDATE last_seen=?, relay_count=relay_count+1, status='ONLINE'");
                long now = System.currentTimeMillis();
                ps.setString(1, nodeId); ps.setString(2, ip);
                ps.setLong(3, now); ps.setLong(4, now);
                ps.executeUpdate();
            } catch (SQLException e) { System.err.println("[DB] Node register failed: " + e.getMessage()); }
        }

        static List<Map<String, String>> fetchNodes() {
            List<Map<String, String>> rows = new ArrayList<>();
            if (conn == null) return rows;
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM mesh_nodes ORDER BY last_seen DESC")) {
                while (rs.next()) {
                    Map<String, String> r = new LinkedHashMap<>();
                    r.put("node_id", rs.getString("node_id"));
                    r.put("ip",      rs.getString("ip_address"));
                    r.put("relays",  String.valueOf(rs.getInt("relay_count")));
                    r.put("status",  rs.getString("status"));
                    long age = System.currentTimeMillis() - rs.getLong("last_seen");
                    r.put("age", formatAge(age));
                    rows.add(r);
                }
            } catch (SQLException e) { /* ignore */ }
            return rows;
        }
    }

    static String formatAge(long ms) {
        if (ms < 60000) return (ms / 1000) + "s ago";
        if (ms < 3600000) return (ms / 60000) + "m ago";
        return (ms / 3600000) + "h ago";
    }

    // ─────────────────────────────────────────────────────────
    //  7. API SERVER
    // ─────────────────────────────────────────────────────────
    static class ApiServer {
        static HttpServer server;
        static final AtomicBoolean online = new AtomicBoolean(false);
        static final CommandQueue queue = new CommandQueue(online);

        static void start() throws IOException {
            server = HttpServer.create(new InetSocketAddress(API_PORT), 0);
            server.createContext("/api/resources", ApiServer::handleResources);
            server.createContext("/api/report",    ApiServer::handleReport);
            server.createContext("/api/nodes",     ApiServer::handleNodes);
            server.createContext("/api/ping",      ApiServer::handlePing);
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            online.set(true);
            System.out.println("[API] Server running on port " + API_PORT);
        }

        static void stop() { if (server != null) server.stop(0); }

        static void send(HttpExchange ex, int code, String body) throws IOException {
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            byte[] b = body.getBytes();
            ex.sendResponseHeaders(code, b.length);
            ex.getResponseBody().write(b);
            ex.getResponseBody().close();
        }

        static void handlePing(HttpExchange ex) throws IOException {
            send(ex, 200, "{\"status\":\"ok\",\"timestamp\":" + System.currentTimeMillis() + "}");
        }

        static void handleResources(HttpExchange ex) throws IOException {
            List<Map<String, Object>> data = Database.fetchResources();
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < data.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("{");
                boolean first = true;
                for (var entry : data.get(i).entrySet()) {
                    if (!first) sb.append(",");
                    sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
                    first = false;
                }
                sb.append("}");
            }
            sb.append("]");
            send(ex, 200, sb.toString());
        }

        static void handleReport(HttpExchange ex) throws IOException {
            if (!"POST".equals(ex.getRequestMethod())) { send(ex, 405, "{}"); return; }
            String body = new String(ex.getRequestBody().readAllBytes());
            Map<String, String> params = parseSimpleJson(body);
            ActionObject action = new ActionObject("REPORT_RESOURCE", params);
            queue.enqueue(action);
            send(ex, 200, "{\"status\":\"queued\",\"action_id\":\"" + action.id + "\"}");
        }

        static void handleNodes(HttpExchange ex) throws IOException {
            String ip = ex.getRemoteAddress().getAddress().getHostAddress();
            Database.registerNode("NODE-" + ip.replace(".", "-"), ip);
            List<Map<String, String>> nodes = Database.fetchNodes();
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < nodes.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("{");
                boolean f = true;
                for (var e : nodes.get(i).entrySet()) {
                    if (!f) sb.append(",");
                    sb.append("\"").append(e.getKey()).append("\":\"").append(e.getValue()).append("\"");
                    f = false;
                }
                sb.append("}");
            }
            sb.append("]");
            send(ex, 200, sb.toString());
        }

        static Map<String, String> parseSimpleJson(String json) {
            Map<String, String> map = new HashMap<>();
            json = json.replaceAll("[{}\"]", "");
            for (String pair : json.split(",")) {
                String[] kv = pair.split(":", 2);
                if (kv.length == 2) map.put(kv[0].trim(), kv[1].trim());
            }
            return map;
        }
    }

    // ─────────────────────────────────────────────────────────
    //  8. DESIGN SYSTEM — COLORS & FONTS
    // ─────────────────────────────────────────────────────────

    // Backgrounds
    static final Color BG_APP      = new Color(246, 247, 250); // main app bg (very light gray)
    static final Color BG_SIDEBAR  = new Color(17,  24,  39);  // deep navy sidebar
    static final Color BG_CARD     = Color.WHITE;
    static final Color BG_INPUT    = new Color(249, 250, 251);
    static final Color BG_HOVER    = new Color(238, 242, 255);
    static final Color BG_SEL      = new Color(224, 231, 255); // sidebar selected

    // Text
    static final Color T_PRIMARY   = new Color(17,  24,  39);
    static final Color T_SECONDARY = new Color(107, 114, 128);
    static final Color T_MUTED     = new Color(156, 163, 175);
    static final Color T_SIDEBAR   = new Color(209, 213, 219);
    static final Color T_SIDEBAR_A = Color.WHITE;

    // Accents
    static final Color C_BLUE      = new Color(59,  130, 246);
    static final Color C_BLUE_D    = new Color(37,  99,  235);
    static final Color C_FRESH     = new Color(22,  163, 74);
    static final Color C_AGING     = new Color(217, 119, 6);
    static final Color C_STALE     = new Color(220, 38,  38);
    static final Color C_PURPLE    = new Color(124, 58,  237);
    static final Color C_TEAL      = new Color(20,  184, 166);

    // Borders
    static final Color BORDER_LIGHT = new Color(229, 231, 235);
    static final Color BORDER_MED   = new Color(209, 213, 219);
    static final Color SIDEBAR_SEP  = new Color(55,  65,  81);

    // Fonts
    static final Font F_DISPLAY = new Font("SansSerif", Font.BOLD, 20);
    static final Font F_TITLE   = new Font("SansSerif", Font.BOLD, 15);
    static final Font F_LABEL   = new Font("SansSerif", Font.BOLD, 13);
    static final Font F_BODY    = new Font("SansSerif", Font.PLAIN, 13);
    static final Font F_SMALL   = new Font("SansSerif", Font.PLAIN, 11);
    static final Font F_MONO    = new Font("Monospaced", Font.PLAIN, 12);
    static final Font F_NAV     = new Font("SansSerif", Font.PLAIN, 13);
    static final Font F_NAV_B   = new Font("SansSerif", Font.BOLD, 13);

    // ─────────────────────────────────────────────────────────
    //  9. GLOBAL UI STATE
    // ─────────────────────────────────────────────────────────
    static JFrame mainFrame;
    static JPanel contentArea;    // right side, swapped by nav
    static CardLayout cardLayout;

    // Nav items
    static JButton navDashboard, navReport, navMesh, navQueue;
    static JButton activeNav = null;

    // Status widgets
    static JLabel lblOnlineBadge, lblQueueBadge, lblStatusMsg;

    // Dashboard
    static DefaultTableModel resourceTableModel;
    static JTable resourceTable;
    static final Map<String, JLabel> statLabels = new HashMap<>();

    // Report form
    static JTextField fName, fAmount, fLocation, fReporter;
    static JComboBox<String> fUnit, fCategory;
    static JLabel previewValue;

    // Mesh
    static JPanel meshListPanel;

    // Queue
    static DefaultTableModel queueTableModel;

    // ─────────────────────────────────────────────────────────
    //  10. MAIN UI BUILD
    // ─────────────────────────────────────────────────────────
    static void buildUI() {
        mainFrame = new JFrame("PDR-Sync — Precision Disaster Recovery");
        mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        mainFrame.setSize(1160, 740);
        mainFrame.setMinimumSize(new Dimension(900, 600));
        mainFrame.setLocationRelativeTo(null);

        JPanel root = new JPanel(new BorderLayout(0, 0));
        root.setBackground(BG_APP);

        root.add(buildSidebar(),  BorderLayout.WEST);
        root.add(buildMain(),     BorderLayout.CENTER);

        mainFrame.setContentPane(root);
        mainFrame.setVisible(true);

        // Show dashboard first
        switchNav(navDashboard, "dashboard");
        refreshDashboard();

        // Auto-refresh
        new javax.swing.Timer(10_000, e -> refreshDashboard()).start();
        new javax.swing.Timer(3_000,  e -> updateStatusBadges()).start();
    }

    // ─────────────────────────────────────────────────────────
    //  11. SIDEBAR
    // ─────────────────────────────────────────────────────────
    static JPanel buildSidebar() {
        JPanel sidebar = new JPanel();
        sidebar.setLayout(new BoxLayout(sidebar, BoxLayout.Y_AXIS));
        sidebar.setBackground(BG_SIDEBAR);
        sidebar.setPreferredSize(new Dimension(220, 0));
        sidebar.setBorder(new EmptyBorder(0, 0, 0, 0));

        // ── Logo block
        JPanel logoBlock = new JPanel(new BorderLayout());
        logoBlock.setOpaque(false);
        logoBlock.setBorder(new EmptyBorder(24, 20, 20, 20));
        logoBlock.setMaximumSize(new Dimension(220, 80));

        JLabel logoText = new JLabel("PDR-Sync");
        logoText.setFont(new Font("SansSerif", Font.BOLD, 18));
        logoText.setForeground(Color.WHITE);

        JLabel logoSub = new JLabel("Disaster Recovery Platform");
        logoSub.setFont(F_SMALL);
        logoSub.setForeground(new Color(156, 163, 175));

        JPanel logoInner = new JPanel(new GridLayout(2, 1, 0, 2));
        logoInner.setOpaque(false);
        logoInner.add(logoText);
        logoInner.add(logoSub);
        logoBlock.add(logoInner, BorderLayout.CENTER);
        sidebar.add(logoBlock);

        // ── Separator
        sidebar.add(sidebarSep());

        // ── Nav section label
        sidebar.add(sidebarSectionLabel("NAVIGATION"));

        // ── Nav items
        navDashboard = navButton("  Dashboard",   "  Overview & resources");
        navReport    = navButton("  Report",      "  Submit field report");
        navMesh      = navButton("  Mesh Nodes",  "  P2P network status");
        navQueue     = navButton("  Queue",       "  Offline action queue");

        navDashboard.addActionListener(e -> switchNav(navDashboard, "dashboard"));
        navReport   .addActionListener(e -> switchNav(navReport,    "report"));
        navMesh     .addActionListener(e -> { switchNav(navMesh, "mesh"); refreshMeshPanel(); });
        navQueue    .addActionListener(e -> { switchNav(navQueue, "queue"); refreshQueueTab(); });

        sidebar.add(navDashboard);
        sidebar.add(navReport);
        sidebar.add(navMesh);
        sidebar.add(navQueue);

        sidebar.add(Box.createVerticalGlue());
        sidebar.add(sidebarSep());

        // ── Status section
        sidebar.add(sidebarSectionLabel("SYSTEM"));
        sidebar.add(buildSidebarStatus());
        sidebar.add(Box.createVerticalStrut(16));

        return sidebar;
    }

    static JPanel sidebarSep() {
        JPanel sep = new JPanel();
        sep.setBackground(SIDEBAR_SEP);
        sep.setMaximumSize(new Dimension(220, 1));
        sep.setPreferredSize(new Dimension(220, 1));
        return sep;
    }

    static JLabel sidebarSectionLabel(String text) {
        JLabel lbl = new JLabel("  " + text);
        lbl.setFont(new Font("SansSerif", Font.BOLD, 10));
        lbl.setForeground(new Color(107, 114, 128));
        lbl.setBorder(new EmptyBorder(14, 0, 6, 0));
        lbl.setMaximumSize(new Dimension(220, 32));
        lbl.setAlignmentX(Component.LEFT_ALIGNMENT);
        return lbl;
    }

    static JButton navButton(String title, String sub) {
        JButton btn = new JButton("<html><b style='font-size:12px'>" + title + "</b><br>" +
            "<span style='color:#9ca3af;font-size:10px'>" + sub + "</span></html>");
        btn.setFont(F_NAV);
        btn.setForeground(T_SIDEBAR);
        btn.setBackground(new Color(0, 0, 0, 0));
        btn.setOpaque(true);
        btn.setBorderPainted(false);
        btn.setFocusPainted(false);
        btn.setHorizontalAlignment(SwingConstants.LEFT);
        btn.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        btn.setMaximumSize(new Dimension(220, 52));
        btn.setAlignmentX(Component.LEFT_ALIGNMENT);
        btn.setBorder(new EmptyBorder(10, 18, 10, 18));
        btn.addMouseListener(new MouseAdapter() {
            public void mouseEntered(MouseEvent e) {
                if (btn != activeNav) btn.setBackground(new Color(31, 41, 55));
            }
            public void mouseExited(MouseEvent e) {
                if (btn != activeNav) btn.setBackground(BG_SIDEBAR);
            }
        });
        return btn;
    }

    static void switchNav(JButton btn, String card) {
        if (activeNav != null) {
            activeNav.setBackground(BG_SIDEBAR);
            activeNav.setForeground(T_SIDEBAR);
        }
        activeNav = btn;
        btn.setBackground(new Color(37, 99, 235));
        btn.setForeground(Color.WHITE);
        cardLayout.show(contentArea, card);
    }

    static JPanel buildSidebarStatus() {
        JPanel p = new JPanel(new GridLayout(3, 1, 0, 6));
        p.setOpaque(false);
        p.setBorder(new EmptyBorder(6, 14, 6, 14));
        p.setMaximumSize(new Dimension(220, 100));

        lblOnlineBadge = new JLabel("● Online");
        lblOnlineBadge.setFont(F_LABEL);
        lblOnlineBadge.setForeground(new Color(74, 222, 128));

        lblQueueBadge = new JLabel("Queue: 0 pending");
        lblQueueBadge.setFont(F_SMALL);
        lblQueueBadge.setForeground(new Color(156, 163, 175));

        JButton btnToggle = new JButton("Toggle Offline");
        btnToggle.setFont(F_SMALL);
        btnToggle.setForeground(new Color(209, 213, 219));
        btnToggle.setBackground(new Color(55, 65, 81));
        btnToggle.setBorder(new EmptyBorder(4, 10, 4, 10));
        btnToggle.setFocusPainted(false);
        btnToggle.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        btnToggle.addActionListener(e -> toggleOnline(lblOnlineBadge));
        btnToggle.addMouseListener(new MouseAdapter() {
            public void mouseEntered(MouseEvent e) { btnToggle.setBackground(new Color(75, 85, 99)); }
            public void mouseExited(MouseEvent e)  { btnToggle.setBackground(new Color(55, 65, 81)); }
        });

        p.add(lblOnlineBadge);
        p.add(lblQueueBadge);
        p.add(btnToggle);
        return p;
    }

    static void updateStatusBadges() {
        SwingUtilities.invokeLater(() -> {
            int p = ApiServer.queue.pending();
            lblQueueBadge.setText(p > 0 ? "Queue: " + p + " pending" : "Queue: synced");
            lblQueueBadge.setForeground(p > 0 ? new Color(251, 191, 36) : new Color(156, 163, 175));
        });
    }

    // ─────────────────────────────────────────────────────────
    //  12. MAIN CONTENT AREA (CardLayout)
    // ─────────────────────────────────────────────────────────
    static JPanel buildMain() {
        JPanel wrapper = new JPanel(new BorderLayout());
        wrapper.setBackground(BG_APP);

        // Top bar
        wrapper.add(buildTopBar(), BorderLayout.NORTH);

        // Card content
        cardLayout  = new CardLayout();
        contentArea = new JPanel(cardLayout);
        contentArea.setBackground(BG_APP);
        contentArea.add(buildDashboardPanel(), "dashboard");
        contentArea.add(buildReportPanel(),    "report");
        contentArea.add(buildMeshPanel_(),     "mesh");
        contentArea.add(buildQueuePanel(),     "queue");

        wrapper.add(contentArea, BorderLayout.CENTER);
        return wrapper;
    }

    static JPanel buildTopBar() {
        JPanel bar = new JPanel(new BorderLayout());
        bar.setBackground(BG_CARD);
        bar.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createMatteBorder(0, 0, 1, 0, BORDER_LIGHT),
            new EmptyBorder(12, 24, 12, 24)));

        lblStatusMsg = new JLabel("System operational  ·  API running on port " + API_PORT);
        lblStatusMsg.setFont(F_BODY);
        lblStatusMsg.setForeground(T_SECONDARY);

        JLabel time = new JLabel(new SimpleDateFormat("EEEE, dd MMM yyyy").format(new Date()));
        time.setFont(F_BODY);
        time.setForeground(T_MUTED);

        bar.add(lblStatusMsg, BorderLayout.WEST);
        bar.add(time, BorderLayout.EAST);

        // Update time every minute
        new javax.swing.Timer(60_000, e -> time.setText(new SimpleDateFormat("EEEE, dd MMM yyyy").format(new Date()))).start();

        return bar;
    }

    // ─────────────────────────────────────────────────────────
    //  13. DASHBOARD PANEL
    // ─────────────────────────────────────────────────────────
    static JPanel buildDashboardPanel() {
        JPanel p = new JPanel(new BorderLayout(0, 0));
        p.setBackground(BG_APP);
        p.setBorder(new EmptyBorder(24, 24, 24, 24));

        // Page title
        JPanel titleRow = new JPanel(new BorderLayout());
        titleRow.setOpaque(false);
        titleRow.setBorder(new EmptyBorder(0, 0, 20, 0));
        JLabel pageTitle = new JLabel("Resource Dashboard");
        pageTitle.setFont(F_DISPLAY);
        pageTitle.setForeground(T_PRIMARY);
        JLabel pageSub = new JLabel("Live view of all reported resources with confidence decay");
        pageSub.setFont(F_BODY);
        pageSub.setForeground(T_SECONDARY);
        JPanel titles = new JPanel(new GridLayout(2, 1, 0, 3));
        titles.setOpaque(false);
        titles.add(pageTitle);
        titles.add(pageSub);
        titleRow.add(titles, BorderLayout.WEST);

        JButton syncBtn = primaryButton("Sync Queue");
        syncBtn.addActionListener(e -> { ApiServer.queue.flush(); refreshDashboard(); setStatus("Queue flushed and synced."); });
        titleRow.add(syncBtn, BorderLayout.EAST);
        p.add(titleRow, BorderLayout.NORTH);

        // Stat cards row
        JPanel statsRow = new JPanel(new GridLayout(1, 4, 14, 0));
        statsRow.setOpaque(false);
        statsRow.setPreferredSize(new Dimension(0, 100));
        statsRow.add(buildStatCard("Total Resources", "0",  C_BLUE,   "total"));
        statsRow.add(buildStatCard("Fresh  (>70%)",   "0",  C_FRESH,  "fresh"));
        statsRow.add(buildStatCard("Aging  (40-70%)", "0",  C_AGING,  "aging"));
        statsRow.add(buildStatCard("Stale  (<40%)",   "0",  C_STALE,  "stale"));

        JPanel top = new JPanel(new BorderLayout(0, 14));
        top.setOpaque(false);
        top.add(titleRow,  BorderLayout.NORTH);
        top.add(statsRow,  BorderLayout.CENTER);
        p.add(top, BorderLayout.NORTH);

        // Table card
        JPanel tableCard = card();
        tableCard.setLayout(new BorderLayout(0, 0));

        JPanel tableHeader = new JPanel(new BorderLayout());
        tableHeader.setOpaque(false);
        tableHeader.setBorder(new EmptyBorder(16, 18, 12, 18));
        JLabel tblTitle = new JLabel("All Reported Resources");
        tblTitle.setFont(F_TITLE);
        tblTitle.setForeground(T_PRIMARY);
        tableHeader.add(tblTitle, BorderLayout.WEST);

        String[] cols = {"#","Resource Name","Category","Raw Amount","Normalized","Location","Reporter","Confidence"};
        resourceTableModel = new DefaultTableModel(cols, 0) {
            public boolean isCellEditable(int r, int c) { return false; }
        };
        resourceTable = new JTable(resourceTableModel);
        styleTable(resourceTable);
        resourceTable.setDefaultRenderer(Object.class, new ResourceTableRenderer());

        // Column widths
        int[] widths = {36, 160, 100, 110, 110, 140, 110, 110};
        for (int i = 0; i < widths.length; i++) {
            resourceTable.getColumnModel().getColumn(i).setPreferredWidth(widths[i]);
        }

        JScrollPane scroll = styledScroll(resourceTable);
        tableCard.add(tableHeader, BorderLayout.NORTH);
        tableCard.add(scroll,      BorderLayout.CENTER);

        p.add(tableCard, BorderLayout.CENTER);
        return p;
    }

    static JPanel buildStatCard(String label, String val, Color accent, String key) {
        JPanel card = card();
        card.setLayout(new BorderLayout(0, 6));
        card.setBorder(new EmptyBorder(18, 20, 18, 20));

        // Accent top bar
        JPanel accentBar = new JPanel();
        accentBar.setBackground(accent);
        accentBar.setPreferredSize(new Dimension(0, 3));

        JLabel lbl = new JLabel(label);
        lbl.setFont(F_SMALL);
        lbl.setForeground(T_SECONDARY);

        JLabel num = new JLabel(val);
        num.setFont(new Font("SansSerif", Font.BOLD, 32));
        num.setForeground(accent);
        statLabels.put(key, num);

        JPanel inner = new JPanel(new GridLayout(2, 1, 0, 4));
        inner.setOpaque(false);
        inner.add(lbl);
        inner.add(num);

        card.add(accentBar, BorderLayout.NORTH);
        card.add(inner,     BorderLayout.CENTER);
        return card;
    }

    // Custom table renderer for confidence badges
    static class ResourceTableRenderer extends DefaultTableCellRenderer {
        public Component getTableCellRendererComponent(JTable t, Object val,
                boolean sel, boolean foc, int row, int col) {
            JLabel c = (JLabel) super.getTableCellRendererComponent(t, val, sel, foc, row, col);
            c.setBorder(new EmptyBorder(4, 12, 4, 12));
            c.setFont(F_BODY);

            if (sel) {
                c.setBackground(new Color(239, 246, 255));
                c.setForeground(T_PRIMARY);
            } else {
                c.setBackground(row % 2 == 0 ? Color.WHITE : new Color(249, 250, 251));
                c.setForeground(T_PRIMARY);
            }

            if (col == 7 && val != null) {  // Confidence column
                try {
                    int pct = Integer.parseInt(val.toString().replace("%", "").trim());
                    Color fg = DecayEngine.confidenceColor(pct);
                    Color bg = DecayEngine.confidenceBg(pct);
                    c.setText(pct + "%  " + DecayEngine.confidenceLabel(pct));
                    c.setForeground(fg);
                    c.setBackground(sel ? new Color(239, 246, 255) : bg);
                    c.setFont(F_LABEL);
                } catch (Exception ignored) {}
            }
            if (col == 2 && val != null) {  // Category — colored pill text
                c.setForeground(C_PURPLE);
                c.setFont(F_LABEL);
            }
            return c;
        }
    }

    static void refreshDashboard() {
        SwingUtilities.invokeLater(() -> {
            List<Map<String, Object>> data = Database.fetchResources();
            resourceTableModel.setRowCount(0);
            int fresh = 0, aging = 0, stale = 0;
            for (Map<String, Object> row : data) {
                int conf = (Integer) row.get("confidence");
                if (conf >= 70) fresh++;
                else if (conf >= 40) aging++;
                else stale++;
                resourceTableModel.addRow(new Object[]{
                    row.get("id"), row.get("name"), row.get("category"),
                    row.get("amount_raw"), row.get("normalized"),
                    row.get("location"), row.get("reporter"), conf
                });
            }
            statLabels.get("total").setText(String.valueOf(data.size()));
            statLabels.get("fresh").setText(String.valueOf(fresh));
            statLabels.get("aging").setText(String.valueOf(aging));
            statLabels.get("stale").setText(String.valueOf(stale));
        });
    }

    // ─────────────────────────────────────────────────────────
    //  14. REPORT PANEL
    // ─────────────────────────────────────────────────────────
    static JPanel buildReportPanel() {
        JPanel outer = new JPanel(new BorderLayout());
        outer.setBackground(BG_APP);
        outer.setBorder(new EmptyBorder(24, 24, 24, 24));

        // Page title
        JLabel title = new JLabel("Report Resource");
        title.setFont(F_DISPLAY);
        title.setForeground(T_PRIMARY);
        JLabel sub = new JLabel("Submit a new resource report from the field");
        sub.setFont(F_BODY);
        sub.setForeground(T_SECONDARY);
        JPanel titleBox = new JPanel(new GridLayout(2,1,0,4));
        titleBox.setOpaque(false);
        titleBox.setBorder(new EmptyBorder(0,0,20,0));
        titleBox.add(title); titleBox.add(sub);
        outer.add(titleBox, BorderLayout.NORTH);

        // Two-column layout: form (left) + preview (right)
        JPanel cols = new JPanel(new GridLayout(1, 2));
        cols.setBorder(new EmptyBorder(0, 0, 20, 0));
        cols.setOpaque(false);

        // LEFT: form card
        JPanel formCard = card();
        formCard.setLayout(new BoxLayout(formCard, BoxLayout.Y_AXIS));
        formCard.setBorder(new EmptyBorder(24, 28, 28, 28));

        formCard.add(formSectionLabel("Resource Details"));
        formCard.add(Box.createVerticalStrut(16));
        formCard.add(formRow("Resource Name", fName = formField("e.g. Bottled Water")));
        formCard.add(Box.createVerticalStrut(12));
        formCard.add(formRow("Category",      fCategory = formCombo(new String[]{"Food","Water","Medical","Fuel","Shelter","Personnel","Equipment"})));
        formCard.add(Box.createVerticalStrut(20));
        formCard.add(formSectionLabel("Quantity"));
        formCard.add(Box.createVerticalStrut(14));

        // Amount + Unit on same row
        JPanel amtRow = new JPanel(new GridLayout(1, 2, 12, 0));
        amtRow.setOpaque(false);
        amtRow.setMaximumSize(new Dimension(Integer.MAX_VALUE, 70));
        fAmount = formField("0");
        fUnit   = formCombo(new String[]{"liters","gallons","kg","lbs","units","cases","boxes","packs","pallets","ml","tons"});
        amtRow.add(formRow("Amount", fAmount));
        amtRow.add(formRow("Unit",   fUnit));
        formCard.add(amtRow);

        formCard.add(Box.createVerticalStrut(20));
        formCard.add(formSectionLabel("Location & Reporter"));
        formCard.add(Box.createVerticalStrut(14));
        formCard.add(formRow("Location",      fLocation = formField("e.g. Sector 7, Grid 4N")));
        formCard.add(Box.createVerticalStrut(12));
        formCard.add(formRow("Reporter Name", fReporter = formField("e.g. Field Team Alpha")));
        formCard.add(Box.createVerticalStrut(24));

        JButton submitBtn = primaryButton("Submit Report");
        submitBtn.setFont(new Font("SansSerif", Font.BOLD, 14));
        submitBtn.setAlignmentX(Component.LEFT_ALIGNMENT);
        submitBtn.setMaximumSize(new Dimension(Integer.MAX_VALUE, 44));
        submitBtn.addActionListener(e -> submitReport());
        formCard.add(submitBtn);

        JScrollPane formScroll = new JScrollPane(formCard);
        formScroll.setBorder(null);
        formScroll.getViewport().setBackground(BG_APP);
        formScroll.getVerticalScrollBar().setUnitIncrement(12);

        cols.add(formScroll);

        // RIGHT: preview card
        JPanel previewCard = card();
        previewCard.setLayout(new BoxLayout(previewCard, BoxLayout.Y_AXIS));
        previewCard.setBorder(new EmptyBorder(24, 24, 24, 24));

        JLabel prevTitle = new JLabel("Normalization Preview");
        prevTitle.setFont(F_TITLE);
        prevTitle.setForeground(T_PRIMARY);
        prevTitle.setAlignmentX(Component.LEFT_ALIGNMENT);
        previewCard.add(prevTitle);

        JLabel prevSub = new JLabel("Live unit conversion output");
        prevSub.setFont(F_SMALL);
        prevSub.setForeground(T_SECONDARY);
        prevSub.setAlignmentX(Component.LEFT_ALIGNMENT);
        previewCard.add(prevSub);
        previewCard.add(Box.createVerticalStrut(20));

        // Conversion box
        JPanel convBox = new JPanel();
        convBox.setBackground(new Color(239, 246, 255));
        convBox.setLayout(new BoxLayout(convBox, BoxLayout.Y_AXIS));
        convBox.setBorder(new EmptyBorder(16, 18, 16, 18));
        convBox.setAlignmentX(Component.LEFT_ALIGNMENT);
        convBox.setMaximumSize(new Dimension(Integer.MAX_VALUE, 80));

        previewValue = new JLabel("Enter amount and unit above");
        previewValue.setFont(new Font("SansSerif", Font.BOLD, 14));
        previewValue.setForeground(C_BLUE);
        previewValue.setAlignmentX(Component.LEFT_ALIGNMENT);

        JLabel convLabel = new JLabel("Normalized output");
        convLabel.setFont(F_SMALL);
        convLabel.setForeground(T_SECONDARY);
        convLabel.setAlignmentX(Component.LEFT_ALIGNMENT);

        convBox.add(convLabel);
        convBox.add(Box.createVerticalStrut(6));
        convBox.add(previewValue);
        previewCard.add(convBox);

        previewCard.add(Box.createVerticalStrut(20));

        // Strategy info boxes
        previewCard.add(infoBox("LiquidStrategy",
            "gallons, liters, ml → liters", new Color(219, 234, 254), C_BLUE));
        previewCard.add(Box.createVerticalStrut(8));
        previewCard.add(infoBox("WeightStrategy",
            "lbs, tons, pallets → kg", new Color(220, 252, 231), C_FRESH));
        previewCard.add(Box.createVerticalStrut(8));
        previewCard.add(infoBox("CountStrategy",
            "cases, boxes, packs → units", new Color(237, 233, 254), C_PURPLE));

        previewCard.add(Box.createVerticalStrut(20));

        // Confidence preview
        JLabel confTitle = new JLabel("Confidence at Submit");
        confTitle.setFont(F_LABEL);
        confTitle.setForeground(T_PRIMARY);
        confTitle.setAlignmentX(Component.LEFT_ALIGNMENT);
        previewCard.add(confTitle);
        previewCard.add(Box.createVerticalStrut(8));

        JPanel confBox = new JPanel(new BorderLayout());
        confBox.setBackground(new Color(220, 252, 231));
        confBox.setBorder(new EmptyBorder(12, 16, 12, 16));
        confBox.setMaximumSize(new Dimension(Integer.MAX_VALUE, 50));
        confBox.setAlignmentX(Component.LEFT_ALIGNMENT);
        JLabel confVal = new JLabel("100%  ● Fresh — just submitted");
        confVal.setFont(F_LABEL);
        confVal.setForeground(C_FRESH);
        confBox.add(confVal);
        previewCard.add(confBox);

        cols.add(previewCard);
        outer.add(cols, BorderLayout.CENTER);

        // Live preview wiring
        ActionListener liveUpdate = e -> {
            try {
                double amt = Double.parseDouble(fAmount.getText().trim());
                String unit = (String) fUnit.getSelectedItem();
                NormalizedResult nr = NormalizationEngine.normalize(amt, unit);
                previewValue.setText(String.format("%.2f %s  (%s)", nr.baseAmount(), nr.baseUnit(), nr.strategyUsed()));
            } catch (Exception ex) {
                previewValue.setText("Enter a valid number");
            }
        };
        fAmount.addKeyListener(new KeyAdapter() {
            public void keyReleased(KeyEvent e) { liveUpdate.actionPerformed(null); }
        });
        fUnit.addActionListener(liveUpdate);

        return outer;
    }

    static JPanel infoBox(String title, String detail, Color bg, Color fg) {
        JPanel box = new JPanel(new GridLayout(2, 1, 0, 2));
        box.setBackground(bg);
        box.setBorder(new EmptyBorder(10, 14, 10, 14));
        box.setMaximumSize(new Dimension(Integer.MAX_VALUE, 52));
        box.setAlignmentX(Component.LEFT_ALIGNMENT);
        JLabel t = new JLabel(title);
        t.setFont(F_LABEL);
        t.setForeground(fg);
        JLabel d = new JLabel(detail);
        d.setFont(F_SMALL);
        d.setForeground(new Color(75, 85, 99));
        box.add(t);
        box.add(d);
        return box;
    }

    static void submitReport() {
        String name = fName.getText().trim();
        String loc  = fLocation.getText().trim();
        String rep  = fReporter.getText().trim();
        if (name.isEmpty() || loc.isEmpty()) {
            showError("Resource Name and Location are required fields.");
            return;
        }
        double amt;
        try { amt = Double.parseDouble(fAmount.getText().trim()); }
        catch (Exception ex) { showError("Amount must be a valid number."); return; }

        Map<String, String> payload = new HashMap<>();
        payload.put("name",     name);
        payload.put("category", (String) fCategory.getSelectedItem());
        payload.put("amount",   String.valueOf(amt));
        payload.put("unit",     (String) fUnit.getSelectedItem());
        payload.put("location", loc);
        payload.put("reporter", rep.isEmpty() ? "Anonymous" : rep);

        ActionObject action = new ActionObject("REPORT_RESOURCE", payload);
        ApiServer.queue.enqueue(action);

        NormalizedResult nr = NormalizationEngine.normalize(amt, (String) fUnit.getSelectedItem());
        previewValue.setText("Submitted: " + nr.display());
        setStatus("Report submitted: " + name + " → " + nr.display());
        refreshDashboard();

        fName.setText(""); fAmount.setText(""); fLocation.setText(""); fReporter.setText("");

        if (!ApiServer.online.get()) {
            showInfo("You are OFFLINE.\nThis action has been queued and will sync automatically when connection is restored.");
        } else {
            showInfo("Report submitted successfully!\n" + name + " normalized to " + nr.display());
        }
    }

    // ─────────────────────────────────────────────────────────
    //  15. MESH PANEL
    // ─────────────────────────────────────────────────────────
    static JPanel buildMeshPanel_() {
        JPanel outer = new JPanel(new BorderLayout());
        outer.setBackground(BG_APP);
        outer.setBorder(new EmptyBorder(24, 24, 24, 24));

        // Title
        JLabel title = new JLabel("Mesh Relay Nodes");
        title.setFont(F_DISPLAY);
        title.setForeground(T_PRIMARY);
        JLabel sub = new JLabel("Devices participating in the P2P relay network (simulated via LAN)");
        sub.setFont(F_BODY);
        sub.setForeground(T_SECONDARY);
        JPanel titleBox = new JPanel(new GridLayout(2,1,0,4));
        titleBox.setOpaque(false);

        JButton simBtn   = secondaryButton("+ Simulate Node");
        JButton refBtn   = secondaryButton("Refresh");
        simBtn.addActionListener(e -> {
            String fakeIp = "192.168." + (int)(Math.random()*10) + "." + (int)(Math.random()*254+1);
            Database.registerNode("NODE-SIM-" + (System.currentTimeMillis() % 10000), fakeIp);
            refreshMeshPanel();
            setStatus("Simulated node joined: " + fakeIp);
        });
        refBtn.addActionListener(e -> refreshMeshPanel());

        JPanel titleRow = new JPanel(new BorderLayout());
        titleRow.setOpaque(false);
        titleRow.setBorder(new EmptyBorder(0,0,20,0));
        titleBox.add(title); titleBox.add(sub);
        titleRow.add(titleBox, BorderLayout.WEST);
        JPanel btnGroup = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        btnGroup.setOpaque(false);
        btnGroup.add(refBtn); btnGroup.add(simBtn);
        titleRow.add(btnGroup, BorderLayout.EAST);
        outer.add(titleRow, BorderLayout.NORTH);

        // Info banner
        JPanel banner = new JPanel(new BorderLayout());
        banner.setBackground(new Color(239, 246, 255));
        banner.setBorder(new EmptyBorder(12, 16, 12, 16));
        banner.setMaximumSize(new Dimension(Integer.MAX_VALUE, 44));
        JLabel bannerText = new JLabel("In production, devices are discovered via Wi-Fi Direct / BLE. Here, any device hitting /api/nodes registers automatically.");
        bannerText.setFont(F_SMALL);
        bannerText.setForeground(new Color(37, 99, 235));
        banner.add(bannerText);

        meshListPanel = new JPanel();
        meshListPanel.setLayout(new BoxLayout(meshListPanel, BoxLayout.Y_AXIS));
        meshListPanel.setBackground(BG_APP);

        JScrollPane scroll = new JScrollPane(meshListPanel);
        scroll.setBorder(null);
        scroll.getViewport().setBackground(BG_APP);

        JPanel center = new JPanel(new BorderLayout(0, 12));
        center.setOpaque(false);
        center.add(banner, BorderLayout.NORTH);
        center.add(scroll, BorderLayout.CENTER);
        outer.add(center, BorderLayout.CENTER);
        return outer;
    }

    static void refreshMeshPanel() {
        SwingUtilities.invokeLater(() -> {
            meshListPanel.removeAll();
            List<Map<String, String>> nodes = Database.fetchNodes();

            if (nodes.isEmpty()) {
                JPanel emptyState = new JPanel(new BorderLayout());
                emptyState.setBackground(BG_CARD);
                emptyState.setBorder(new EmptyBorder(40, 0, 40, 0));
                JLabel empty = new JLabel("No mesh nodes detected yet", SwingConstants.CENTER);
                empty.setFont(F_BODY);
                empty.setForeground(T_MUTED);
                emptyState.add(empty);
                meshListPanel.add(emptyState);
            }

            for (Map<String, String> node : nodes) {
                meshListPanel.add(buildMeshNodeCard(node));
                meshListPanel.add(Box.createVerticalStrut(8));
            }
            meshListPanel.revalidate();
            meshListPanel.repaint();
        });
    }

    static JPanel buildMeshNodeCard(Map<String, String> node) {
        JPanel card = card();
        card.setLayout(new BorderLayout(16, 0));
        card.setBorder(new EmptyBorder(16, 20, 16, 20));
        card.setMaximumSize(new Dimension(Integer.MAX_VALUE, 72));

        // Status dot
        JPanel dot = new JPanel() {
            protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                Graphics2D g2 = (Graphics2D) g;
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                g2.setColor(C_FRESH);
                g2.fillOval(4, 4, 12, 12);
            }
        };
        dot.setPreferredSize(new Dimension(20, 20));
        dot.setOpaque(false);

        JLabel nodeId = new JLabel(node.get("node_id"));
        nodeId.setFont(F_LABEL);
        nodeId.setForeground(T_PRIMARY);

        JLabel nodeDetail = new JLabel(node.get("ip") + "   ·   " + node.get("relays") + " relays   ·   seen " + node.get("age"));
        nodeDetail.setFont(F_BODY);
        nodeDetail.setForeground(T_SECONDARY);

        JPanel left = new JPanel(new GridLayout(2,1,0,4));
        left.setOpaque(false);
        left.add(nodeId);
        left.add(nodeDetail);

        JPanel dotWrap = new JPanel(new FlowLayout(FlowLayout.CENTER, 0, 20));
        dotWrap.setOpaque(false);
        dotWrap.add(dot);

        JPanel statusBadge = pill(node.get("status"), new Color(220, 252, 231), C_FRESH);

        card.add(dotWrap,     BorderLayout.WEST);
        card.add(left,        BorderLayout.CENTER);
        card.add(statusBadge, BorderLayout.EAST);
        return card;
    }

    // ─────────────────────────────────────────────────────────
    //  16. QUEUE PANEL
    // ─────────────────────────────────────────────────────────
    static JPanel buildQueuePanel() {
        JPanel outer = new JPanel(new BorderLayout());
        outer.setBackground(BG_APP);
        outer.setBorder(new EmptyBorder(24, 24, 24, 24));

        // Title row
        JLabel title = new JLabel("Command Queue");
        title.setFont(F_DISPLAY);
        title.setForeground(T_PRIMARY);
        JLabel sub = new JLabel("Offline action log — actions replay automatically when connection is restored");
        sub.setFont(F_BODY);
        sub.setForeground(T_SECONDARY);

        JButton flush   = primaryButton("Flush & Sync");
        JButton refresh = secondaryButton("Refresh");
        flush.addActionListener(e -> { ApiServer.queue.flush(); refreshQueueTab(); refreshDashboard(); setStatus("Queue flushed."); });
        refresh.addActionListener(e -> refreshQueueTab());

        JPanel titleRow = new JPanel(new BorderLayout());
        titleRow.setOpaque(false);
        titleRow.setBorder(new EmptyBorder(0,0,20,0));
        JPanel titleBox = new JPanel(new GridLayout(2,1,0,4));
        titleBox.setOpaque(false);
        titleBox.add(title); titleBox.add(sub);
        titleRow.add(titleBox, BorderLayout.WEST);
        JPanel btns = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        btns.setOpaque(false);
        btns.add(refresh); btns.add(flush);
        titleRow.add(btns, BorderLayout.EAST);
        outer.add(titleRow, BorderLayout.NORTH);

        // Table
        JPanel tableCard = card();
        tableCard.setLayout(new BorderLayout(0, 0));

        JPanel tableHead = new JPanel(new BorderLayout());
        tableHead.setOpaque(false);
        tableHead.setBorder(new EmptyBorder(16, 18, 12, 18));
        JLabel tblTitle = new JLabel("Queued Actions");
        tblTitle.setFont(F_TITLE);
        tblTitle.setForeground(T_PRIMARY);
        tableHead.add(tblTitle, BorderLayout.WEST);
        tableCard.add(tableHead, BorderLayout.NORTH);

        String[] cols = {"Action ID","Type","Queued At","Payload Preview","Status"};
        queueTableModel = new DefaultTableModel(cols, 0) {
            public boolean isCellEditable(int r, int c) { return false; }
        };
        JTable qt = new JTable(queueTableModel);
        styleTable(qt);
        qt.setDefaultRenderer(Object.class, new QueueTableRenderer());

        tableCard.add(styledScroll(qt), BorderLayout.CENTER);
        outer.add(tableCard, BorderLayout.CENTER);

        new javax.swing.Timer(3000, e -> refreshQueueTab()).start();
        return outer;
    }

    static class QueueTableRenderer extends DefaultTableCellRenderer {
        public Component getTableCellRendererComponent(JTable t, Object val,
                boolean sel, boolean foc, int row, int col) {
            JLabel c = (JLabel) super.getTableCellRendererComponent(t, val, sel, foc, row, col);
            c.setBorder(new EmptyBorder(6, 12, 6, 12));
            c.setFont(F_BODY);
            c.setBackground(row % 2 == 0 ? Color.WHITE : new Color(249, 250, 251));
            c.setForeground(T_PRIMARY);
            if (sel) { c.setBackground(new Color(239, 246, 255)); }

            if (col == 4 && val != null) {  // Status column
                boolean synced = val.toString().contains("YES") || val.toString().contains("Synced");
                c.setFont(F_LABEL);
                c.setText(synced ? "✓  Synced" : "⏳  Pending");
                c.setForeground(synced ? C_FRESH : C_AGING);
                c.setBackground(synced ? new Color(220, 252, 231) : new Color(254, 243, 199));
            }
            if (col == 0) { // ID column
                c.setFont(F_MONO);
                c.setForeground(T_SECONDARY);
            }
            return c;
        }
    }

    static void refreshQueueTab() {
        SwingUtilities.invokeLater(() -> {
            queueTableModel.setRowCount(0);
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
            for (ActionObject a : ApiServer.queue.snapshot()) {
                String preview = a.payload.toString();
                if (preview.length() > 70) preview = preview.substring(0, 70) + "...";
                queueTableModel.addRow(new Object[]{
                    a.id, a.type, sdf.format(new Date(a.timestamp)),
                    preview, a.synced ? "✓ YES" : "⏳ Pending"
                });
            }
        });
    }

    // ─────────────────────────────────────────────────────────
    //  17. ONLINE / OFFLINE TOGGLE
    // ─────────────────────────────────────────────────────────
    static void toggleOnline(JLabel badge) {
        boolean nowOnline = !ApiServer.online.get();
        ApiServer.online.set(nowOnline);
        if (nowOnline) {
            badge.setText("● Online");
            badge.setForeground(new Color(74, 222, 128));
            setStatus("Connection restored. Replaying command queue…");
            ApiServer.queue.flush();
        } else {
            badge.setText("○ Offline");
            badge.setForeground(new Color(252, 165, 165));
            setStatus("OFFLINE mode active. All actions are queued locally.");
        }
    }

    static void setStatus(String msg) {
        SwingUtilities.invokeLater(() -> {
            String ts = new SimpleDateFormat("HH:mm:ss").format(new Date());
            lblStatusMsg.setText(ts + "  ·  " + msg);
        });
    }

    // ─────────────────────────────────────────────────────────
    //  18. UI COMPONENT HELPERS
    // ─────────────────────────────────────────────────────────
    static JPanel card() {
        JPanel p = new JPanel();
        p.setBackground(BG_CARD);
        p.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createLineBorder(BORDER_LIGHT, 1),
            new EmptyBorder(0, 0, 0, 0)));
        return p;
    }

    static JButton primaryButton(String text) {
        JButton b = new JButton(text);
        b.setFont(F_LABEL);
        b.setForeground(Color.WHITE);
        b.setBackground(C_BLUE);
        b.setBorder(new EmptyBorder(9, 18, 9, 18));
        b.setFocusPainted(false);
        b.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        b.addMouseListener(new MouseAdapter() {
            public void mouseEntered(MouseEvent e) { b.setBackground(C_BLUE_D); }
            public void mouseExited(MouseEvent e)  { b.setBackground(C_BLUE); }
        });
        return b;
    }

    static JButton secondaryButton(String text) {
        JButton b = new JButton(text);
        b.setFont(F_BODY);
        b.setForeground(T_PRIMARY);
        b.setBackground(BG_CARD);
        b.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createLineBorder(BORDER_MED, 1),
            new EmptyBorder(8, 16, 8, 16)));
        b.setFocusPainted(false);
        b.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        b.addMouseListener(new MouseAdapter() {
            public void mouseEntered(MouseEvent e) { b.setBackground(new Color(243, 244, 246)); }
            public void mouseExited(MouseEvent e)  { b.setBackground(BG_CARD); }
        });
        return b;
    }

    static JTextField formField(String placeholder) {
        JTextField f = new JTextField(20);
        f.setFont(F_BODY);
        f.setForeground(T_PRIMARY);
        f.setBackground(BG_INPUT);
        f.setCaretColor(C_BLUE);
        f.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createLineBorder(BORDER_MED, 1),
            new EmptyBorder(9, 12, 9, 12)));
        // Placeholder simulation
        f.setText(placeholder);
        f.setForeground(T_MUTED);
        f.addFocusListener(new FocusAdapter() {
            public void focusGained(FocusEvent e) {
                if (f.getText().equals(placeholder)) { f.setText(""); f.setForeground(T_PRIMARY); }
            }
            public void focusLost(FocusEvent e) {
                if (f.getText().isEmpty()) { f.setText(placeholder); f.setForeground(T_MUTED); }
            }
        });
        f.addMouseListener(new MouseAdapter() {
            public void mouseEntered(MouseEvent e) {
                if (!f.isFocusOwner()) f.setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(BORDER_MED, 1),
                    new EmptyBorder(9, 12, 9, 12)));
            }
        });
        return f;
    }

    // Helper to get actual text (not placeholder)
    static String fieldText(JTextField f) {
        String t = f.getText().trim();
        // Check if it's the placeholder (shown in muted color)
        if (f.getForeground().equals(T_MUTED)) return "";
        return t;
    }

    static JComboBox<String> formCombo(String[] items) {
        JComboBox<String> c = new JComboBox<>(items);
        c.setFont(F_BODY);
        c.setBackground(BG_INPUT);
        c.setForeground(T_PRIMARY);
        c.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createLineBorder(BORDER_MED, 1),
            new EmptyBorder(4, 6, 4, 6)));
        return c;
    }

    static JLabel formSectionLabel(String text) {
        JLabel l = new JLabel(text);
        l.setFont(new Font("SansSerif", Font.BOLD, 11));
        l.setForeground(T_MUTED);
        l.setAlignmentX(Component.LEFT_ALIGNMENT);
        return l;
    }

    static JPanel formRow(String label, Component input) {
        JPanel row = new JPanel(new GridLayout(2, 1, 0, 6));
        row.setOpaque(false);
        row.setAlignmentX(Component.LEFT_ALIGNMENT);
        row.setMaximumSize(new Dimension(Integer.MAX_VALUE, 90));
        row.setPreferredSize(new Dimension(0, 90));

        JLabel lbl = new JLabel(label);
        lbl.setFont(F_LABEL);
        lbl.setForeground(T_PRIMARY);

        row.add(lbl);
        row.add(input);
        return row;
    }

    static void styleTable(JTable t) {
        t.setFont(F_BODY);
        t.setRowHeight(38);
        t.setShowGrid(false);
        t.setIntercellSpacing(new Dimension(0, 0));
        t.setSelectionBackground(new Color(239, 246, 255));
        t.setSelectionForeground(T_PRIMARY);
        t.setFillsViewportHeight(true);
        t.setBackground(Color.WHITE);
        t.setForeground(T_PRIMARY);

        JTableHeader header = t.getTableHeader();
        header.setFont(new Font("SansSerif", Font.BOLD, 12));
        header.setBackground(new Color(249, 250, 251));
        header.setForeground(T_SECONDARY);
        header.setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, BORDER_LIGHT));
        header.setPreferredSize(new Dimension(0, 38));
        header.setReorderingAllowed(false);
    }

    static JScrollPane styledScroll(JComponent c) {
        JScrollPane s = new JScrollPane(c);
        s.setBorder(BorderFactory.createMatteBorder(1, 0, 0, 0, BORDER_LIGHT));
        s.getViewport().setBackground(Color.WHITE);
        return s;
    }

    static JPanel pill(String text, Color bg, Color fg) {
        JPanel p = new JPanel(new FlowLayout(FlowLayout.CENTER, 0, 0));
        p.setBackground(bg);
        p.setBorder(new EmptyBorder(4, 10, 4, 10));
        JLabel l = new JLabel(text);
        l.setFont(F_SMALL);
        l.setForeground(fg);
        p.add(l);
        return p;
    }

    static void showError(String msg) {
        JOptionPane.showMessageDialog(mainFrame, msg, "Validation Error", JOptionPane.WARNING_MESSAGE);
    }

    static void showInfo(String msg) {
        JOptionPane.showMessageDialog(mainFrame, msg, "PDR-Sync", JOptionPane.INFORMATION_MESSAGE);
    }

    // ─────────────────────────────────────────────────────────
    //  19. MAIN ENTRY POINT
    // ─────────────────────────────────────────────────────────
    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════╗");
        System.out.println("║  PDR-Sync — Starting Up...   ║");
        System.out.println("╚══════════════════════════════╝");

        boolean dbOk = Database.connect();
        if (!dbOk) System.err.println("[WARN] DB unavailable — offline-only mode.");

        try { ApiServer.start(); }
        catch (IOException e) { System.err.println("[ERROR] API server failed: " + e.getMessage()); }

        SwingUtilities.invokeLater(() -> {
            // Use Nimbus look and feel for better rendering
            try {
                for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                    if ("Nimbus".equals(info.getName())) {
                        UIManager.setLookAndFeel(info.getClassName());
                        // Override Nimbus defaults
                        UIManager.put("control",           BG_APP);
                        UIManager.put("Table.background",  Color.WHITE);
                        UIManager.put("Table.alternateRowColor", new Color(249, 250, 251));
                        break;
                    }
                }
            } catch (Exception ignored) {}
            buildUI();
        });

        try {
            String localIp = InetAddress.getLocalHost().getHostAddress();
            Database.registerNode("NODE-LOCAL-" + localIp, localIp);
        } catch (Exception e) {
            Database.registerNode("NODE-LOCAL", "127.0.0.1");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[PDR-Sync] Shutting down...");
            ApiServer.stop();
        }));
    }
}