-- ============================================================
--  PDR-Sync Database Setup Script
--  Run this ONCE before starting the application
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS pdrsync
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE pdrsync;

-- Resources table (normalized disaster resources)
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
    status       VARCHAR(50) DEFAULT 'ACTIVE',
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Command queue (offline action log)
CREATE TABLE IF NOT EXISTS action_queue (
    id          VARCHAR(36) PRIMARY KEY,
    type        VARCHAR(100),
    payload     TEXT,
    queued_at   BIGINT,
    synced      BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Mesh nodes registry (P2P relay simulation)
CREATE TABLE IF NOT EXISTS mesh_nodes (
    node_id     VARCHAR(100) PRIMARY KEY,
    ip_address  VARCHAR(50),
    last_seen   BIGINT,
    relay_count INT DEFAULT 0,
    status      VARCHAR(50) DEFAULT 'ONLINE',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed some demo data for immediate testing
INSERT INTO resources (name, category, amount_raw, unit_raw, amount_base, unit_base, strategy, location, reporter, reported_at, confidence)
VALUES
  ('Bottled Water', 'Water',    200, 'gallons', 757.00, 'liters', 'LiquidStrategy', 'Camp Alpha - Grid 4N',  'Team Bravo', UNIX_TIMESTAMP(NOW())*1000 - 3600000,  72),
  ('Rice Bags',     'Food',      50, 'cases',   600.00, 'units',  'CountStrategy',  'Distribution Point 2', 'NGO Unity',  UNIX_TIMESTAMP(NOW())*1000 - 7200000,  45),
  ('Diesel Fuel',   'Fuel',     500, 'liters',  500.00, 'liters', 'LiquidStrategy', 'Depot North',           'Admin Cmd',  UNIX_TIMESTAMP(NOW())*1000 - 300000,   98),
  ('First Aid Kits','Medical',   30, 'boxes',   180.00, 'units',  'CountStrategy',  'Medical Tent 3',        'Dr. Priya',  UNIX_TIMESTAMP(NOW())*1000 - 14400000, 10),
  ('Blankets',      'Shelter',  120, 'units',   120.00, 'units',  'CountStrategy',  'Relief Camp B',         'Coord Team', UNIX_TIMESTAMP(NOW())*1000 - 5400000,  58);

-- Verify setup
SELECT 'Database setup complete!' AS status;
SELECT COUNT(*) AS seed_records FROM resources;
