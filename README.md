A local-first disaster coordination platform that enables field teams to report and track resources in zero-bandwidth environments, with automated unit normalization, confidence decay scoring, and offline-first P2P mesh sync. PDR-Sync (Precision Disaster Recovery Sync) is a local-first coordination platform built for disaster response operations where stable internet infrastructure cannot be assumed.

The system addresses three core failures in existing disaster management tools:

Infrastructure Dependency: Field data is captured and queued locally using a Command Queue architecture, ensuring zero data loss during network outages. Actions are replayed to the database in exact chronological order upon reconnection.
Data Heterogeneity: A Normalization Engine built on the Strategy Pattern converts all resource inputs (gallons, pallets, cases, lbs) into standardized base units (liters, kg, units), enabling accurate cross-agency supply comparisons.
Information Decay: A time-based Decay Engine attaches a live confidence percentage to every report. Data degrades from 100% to 0% over four hours, surfacing stale reports visually on the dashboard before responders act on them.
Tech Stack:

Frontend: Java Swing (single-file desktop UI, sidebar navigation, card-based dashboard)
Backend: Embedded Java HTTP server (com.sun.net.httpserver, port 8080, no external framework)
Database: MySQL 8 via JDBC (resources, action_queue, mesh_nodes tables)
Networking: LAN-based P2P mesh simulation; any device contacting /api/nodes is registered as a relay node
Design Patterns Implemented:

Strategy Pattern: NormalizationEngine dispatches to LiquidStrategy, WeightStrategy, or CountStrategy based on the submitted unit type
Command Pattern: Every user action is encapsulated as an ActionObject and stored in a FIFO CommandQueue for offline replay
Observer Pattern: Swing Timers trigger live dashboard refresh and queue status polling
This project is developed as part of a Software Construction course to demonstrate clean architecture, design pattern application, and resilient system design under real-world constraints.
