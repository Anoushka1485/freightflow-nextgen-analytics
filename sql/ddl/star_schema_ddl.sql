-- ============================================================
-- FreightFlow NextGen Analytics
-- Star Schema DDL — Gold Layer (Snowflake)
-- Kimball Dimensional Model | Grain: One row per Shipment
-- ============================================================
--
-- SOURCE TABLE COVERAGE (12 tables → Gold layer):
--
--   Silver Table          → Gold Destination
--   ─────────────────────────────────────────────────────────
--   Vehicle               → dim_vehicle
--   Trucks                → dim_vehicle (merged)
--   Trailers              → dim_vehicle (merged)
--   Service_Event         → dim_vehicle_service
--   Driver                → dim_driver
--   Client                → dim_client
--   Third_Party_Carrier   → dim_carrier
--   Tender                → fact_shipments (weight_lbs, client_key)
--   Shipment              → fact_shipments (core grain)
--   Internal_Shipment     → fact_shipments (fuel_cost_usd)
--   Brokered_Shipment     → fact_shipments (rate_usd, profit_margin_usd, carrier_key)
--   Dispatch              → ETL resolution bridge (see note below)
--
-- DISPATCH RESOLUTION STRATEGY (Option 1 — ETL bridge):
--   Dispatch is a Silver-layer junction table linking Shipment <-> Driver <-> Truck <-> Trailer.
--   It does NOT get its own Gold table. Instead, during ETL load of fact_shipments:
--
--   driver_key  is resolved via:
--     Dispatch.Driver_ID  -> dim_driver.driver_id   -> dim_driver.driver_key
--
--   vehicle_key is resolved via:
--     Dispatch.Truck_ID   -> dim_vehicle.vehicle_id -> dim_vehicle.vehicle_key
--
--   Dispatch.Trailer_ID is also available and can cross-validate vehicle_key
--   or be stored as a degenerate dim if trailer-level analysis is needed.
--
--   ETL pseudocode for resolution:
--     SELECT
--         s.Shipment_ID,
--         d.driver_key,
--         v.vehicle_key
--     FROM silver.Shipment s
--     LEFT JOIN silver.Dispatch disp ON disp.Shipment_ID = s.Shipment_ID
--     LEFT JOIN gold.dim_driver  d   ON d.driver_id  = disp.Driver_ID  AND d.is_current = TRUE
--     LEFT JOIN gold.dim_vehicle v   ON v.vehicle_id = disp.Truck_ID   AND v.is_current = TRUE
-- ============================================================

-- ── dim_date ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key        INT             NOT NULL,   -- YYYYMMDD format e.g. 20260307
    full_date       DATE            NOT NULL,
    day             INT             NOT NULL,
    month           INT             NOT NULL,
    month_name      VARCHAR(20)     NOT NULL,
    quarter         INT             NOT NULL,
    year            INT             NOT NULL,
    weekday_name    VARCHAR(20)     NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

-- ── dim_driver ───────────────────────────────────────────────
-- Source: Driver (Silver)
-- Populated via Dispatch.Driver_ID resolution during fact_shipments ETL
CREATE TABLE IF NOT EXISTS gold.dim_driver (
    driver_key              BIGINT          NOT NULL AUTOINCREMENT,
    driver_id               INT             NOT NULL,   -- NK from Driver
    first_name              VARCHAR(50),
    last_name               VARCHAR(50),
    license_number          VARCHAR(50),
    license_expiry_date     DATE,
    hos_status              VARCHAR(50),                -- Compliant / Non-Compliant
    availability            VARCHAR(50),                -- Available / Unavailable
    city                    VARCHAR(50),
    state                   VARCHAR(50),
    -- SCD Type 1: overwrite on change
    effective_date          DATE            NOT NULL,
    expiry_date             DATE,
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_driver PRIMARY KEY (driver_key)
);

-- ── dim_vehicle ──────────────────────────────────────────────
-- Consolidated from Vehicle + Trucks + Trailers (Silver)
-- Populated via Dispatch.Truck_ID resolution during fact_shipments ETL
CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
    vehicle_key             BIGINT          NOT NULL AUTOINCREMENT,
    vehicle_id              VARCHAR(20)     NOT NULL,   -- NK from Vehicle
    vehicle_type            VARCHAR(10),                -- Truck / Trailer
    vehicle_status          VARCHAR(50),                -- Active / Dormant / Parked / UnderMaintenance
    registration_number     VARCHAR(50),
    -- Truck attributes (NULL if Trailer)
    engine_type             VARCHAR(50),
    current_mileage_k       INT,
    -- Trailer attributes (NULL if Truck)
    trailer_type            VARCHAR(50),
    has_refrigeration       BOOLEAN,
    capacity_lbs            DECIMAL(10,2),
    -- Service summary (denormalized from dim_vehicle_service)
    last_service_date       DATE,
    next_service_date       DATE,
    -- SCD Type 2: track history
    effective_date          DATE            NOT NULL,
    expiry_date             DATE,
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_vehicle PRIMARY KEY (vehicle_key)
);

-- ── dim_vehicle_service (dependent dimension - weak entity) ──
-- Source: Service_Event (Silver)
-- Linked to dim_vehicle via vehicle_key. NOT referenced from fact_shipments directly.
CREATE TABLE IF NOT EXISTS gold.dim_vehicle_service (
    service_key             BIGINT          NOT NULL AUTOINCREMENT,
    vehicle_key             BIGINT          NOT NULL,   -- FK -> dim_vehicle
    service_event_id        INT             NOT NULL,   -- NK from Service_Event
    service_date            TIMESTAMP,
    service_type            VARCHAR(50),                -- Repair / Maintenance / Tires / Replace_Part
    event_description       VARCHAR(200),
    outcome                 VARCHAR(50),                -- Ready_for_Dispatch / Under_Maintenance
    mechanic_cost_usd       DECIMAL(10,2),
    CONSTRAINT pk_dim_vehicle_service PRIMARY KEY (service_key),
    CONSTRAINT fk_service_vehicle FOREIGN KEY (vehicle_key)
        REFERENCES gold.dim_vehicle (vehicle_key)
);

-- ── dim_client ───────────────────────────────────────────────
-- Source: Client (Silver), linked via Tender.Client_ID -> fact_shipments.client_key
CREATE TABLE IF NOT EXISTS gold.dim_client (
    client_key              BIGINT          NOT NULL AUTOINCREMENT,
    client_id               INT             NOT NULL,   -- NK from Client
    company_name            VARCHAR(100),
    point_of_contact        VARCHAR(100),
    billing_address         VARCHAR(100),
    -- SCD Type 1
    effective_date          DATE            NOT NULL,
    expiry_date             DATE,
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_client PRIMARY KEY (client_key)
);

-- ── dim_carrier ──────────────────────────────────────────────
-- Source: Third_Party_Carrier (Silver)
CREATE TABLE IF NOT EXISTS gold.dim_carrier (
    carrier_key             BIGINT          NOT NULL AUTOINCREMENT,
    carrier_id              INT             NOT NULL,   -- NK from Third_Party_Carrier
    carrier_name            VARCHAR(100),
    point_of_contact        VARCHAR(100),
    compliance_status       VARCHAR(50),                -- Compliant / Non-Compliant
    -- SCD Type 1
    effective_date          DATE            NOT NULL,
    expiry_date             DATE,
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_carrier PRIMARY KEY (carrier_key)
);

-- ── dim_location ─────────────────────────────────────────────
-- Reusable for both pickup and delivery points.
-- Sourced from Shipment.Pickup_Point and Shipment.Delivery_Point (Silver).
-- No SCD: address string is the natural key and treated as immutable.
CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key            BIGINT          NOT NULL AUTOINCREMENT,
    full_address            VARCHAR(200)    NOT NULL,   -- NK
    city                    VARCHAR(100),
    state                   VARCHAR(100),
    zip_code                VARCHAR(10),
    CONSTRAINT pk_dim_location PRIMARY KEY (location_key)
);

-- ── fact_shipments ───────────────────────────────────────────
-- Grain: One row per Shipment
--
-- Key resolution during ETL:
--   driver_key          <- Dispatch.Driver_ID  -> dim_driver.driver_id   -> driver_key
--   vehicle_key         <- Dispatch.Truck_ID   -> dim_vehicle.vehicle_id -> vehicle_key
--   client_key          <- Tender.Client_ID    -> dim_client.client_id   -> client_key
--   carrier_key         <- Brokered_Shipment.Carrier_ID -> dim_carrier.carrier_id -> carrier_key
--   pickup_location_key <- Shipment.Pickup_Point  -> dim_location.full_address -> location_key
--   delivery_location_key <- Shipment.Delivery_Point -> dim_location.full_address -> location_key
--   delivery_date_key   <- Shipment.Delivery_Date -> YYYYMMDD INT
--
CREATE TABLE IF NOT EXISTS gold.fact_shipments (
    shipment_key            BIGINT          NOT NULL AUTOINCREMENT,
    -- Foreign Keys
    driver_key              BIGINT,                     -- FK -> dim_driver  | via Dispatch.Driver_ID (NULL for brokered)
    vehicle_key             BIGINT,                     -- FK -> dim_vehicle | via Dispatch.Truck_ID  (NULL for brokered)
    client_key              BIGINT          NOT NULL,   -- FK -> dim_client  | via Tender.Client_ID
    carrier_key             BIGINT,                     -- FK -> dim_carrier | via Brokered_Shipment.Carrier_ID (NULL for internal)
    pickup_location_key     BIGINT          NOT NULL,   -- FK -> dim_location | Shipment.Pickup_Point
    delivery_location_key   BIGINT          NOT NULL,   -- FK -> dim_location | Shipment.Delivery_Point
    delivery_date_key       INT             NOT NULL,   -- FK -> dim_date     | Shipment.Delivery_Date -> YYYYMMDD
    -- Natural Keys (retained for lineage and debugging)
    shipment_id             INT             NOT NULL,   -- NK from Shipment
    tender_id               INT             NOT NULL,   -- NK from Tender
    -- Shipment Type Flag (derived: Internal_Shipment vs Brokered_Shipment)
    shipment_type           VARCHAR(20)     NOT NULL,   -- Internal / Brokered
    -- Degenerate Dimensions
    delivery_status         VARCHAR(50),                -- Delivered / Pending / In_Transit
    temp_requirements       VARCHAR(50),                -- Ambient / Frozen / Deep Frozen / Controlled Cool
    -- Measures
    distance_miles          DECIMAL(10,2),              -- Shipment.Distance
    weight_lbs              DECIMAL(10,2),              -- Tender.Weight
    fuel_cost_usd           DECIMAL(10,2),              -- Internal_Shipment.Fuel_Costs  (NULL for brokered)
    rate_usd                DECIMAL(10,2),              -- Brokered_Shipment.Rate        (NULL for internal)
    profit_margin_usd       DECIMAL(10,2),              -- Brokered_Shipment.Profit_Margin (NULL for internal)
    -- Audit
    etl_load_timestamp      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_shipments        PRIMARY KEY (shipment_key),
    CONSTRAINT fk_fact_driver           FOREIGN KEY (driver_key)            REFERENCES gold.dim_driver   (driver_key),
    CONSTRAINT fk_fact_vehicle          FOREIGN KEY (vehicle_key)           REFERENCES gold.dim_vehicle  (vehicle_key),
    CONSTRAINT fk_fact_client           FOREIGN KEY (client_key)            REFERENCES gold.dim_client   (client_key),
    CONSTRAINT fk_fact_carrier          FOREIGN KEY (carrier_key)           REFERENCES gold.dim_carrier  (carrier_key),
    CONSTRAINT fk_fact_pickup           FOREIGN KEY (pickup_location_key)   REFERENCES gold.dim_location (location_key),
    CONSTRAINT fk_fact_delivery         FOREIGN KEY (delivery_location_key) REFERENCES gold.dim_location (location_key),
    CONSTRAINT fk_fact_date             FOREIGN KEY (delivery_date_key)     REFERENCES gold.dim_date     (date_key)
);

-- ============================================================
-- ADDITIONAL NOTES
-- ============================================================
-- 1. dim_vehicle_service is a dependent dimension (weak entity per ER model).
--    NOT referenced from fact_shipments. Query via:
--    SELECT * FROM gold.dim_vehicle_service WHERE vehicle_key = <x>
--
-- 2. SCD Strategy:
--    dim_driver   -> SCD Type 1 (overwrite)
--    dim_vehicle  -> SCD Type 2 (track history — vehicle status changes matter)
--    dim_client   -> SCD Type 1
--    dim_carrier  -> SCD Type 1
--    dim_location -> No SCD (immutable)
--    dim_date     -> Static (pre-populated for 10+ years)
--
-- 3. NULL FK handling in fact_shipments:
--    Internal shipments -> carrier_key IS NULL
--    Brokered shipments -> driver_key, vehicle_key MAY BE NULL
--    Recommended: insert surrogate -1 row in each dim for 'N/A'
--    and use COALESCE(driver_key, -1) during load.
--
-- 4. dim_location serves dual role:
--    pickup_location_key   -> Shipment.Pickup_Point
--    delivery_location_key -> Shipment.Delivery_Point
--    Both FK columns reference the same dim_location table.
-- ============================================================
