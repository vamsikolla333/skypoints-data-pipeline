-- =============================================================================
-- SkyPoints Global Airline Loyalty Program - Snowflake DDL
-- Medallion Architecture: RAW (Bronze) -> STAGING (Silver) -> MART (Gold)
-- Designed for billion-row daily ingestion via Snowpipe / Streams / Tasks
-- =============================================================================

CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.RAW;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS SKYPOINTS_DB.MART;

-- =============================================================================
-- 1. RAW / LANDING LAYER (Bronze)
-- Stores raw strings as-is to prevent ingestion failures on dirty source data
-- (e.g., stripped leading zeros in DOB, invalid dates like '2021-13-13', spaces).
-- =============================================================================

CREATE OR REPLACE TABLE SKYPOINTS_DB.RAW.MEMBERS_RAW (
    record_type       VARCHAR(2),       -- 'D' for detail record
    member_name       VARCHAR(255),
    member_id         VARCHAR(18),      -- True business key (spec erroneously marks Member_Name as key)
    enrollment_date   VARCHAR(20),      -- Raw string: YYYYMMDD or MMDDYYYY depending on feed
    last_flight_date  VARCHAR(20),
    tier_code         VARCHAR(10),
    agent_name        VARCHAR(255),
    state             VARCHAR(20),
    country           VARCHAR(10),      -- Normalized later (e.g., AU -> AUS, PHIL -> PHL)
    post_code         VARCHAR(10),      -- Present in design spec (pos 9), nullable as omitted in flat file
    dob               VARCHAR(20),      -- Raw string: DDMMYYYY (avoid INT to preserve leading zero)
    is_active         VARCHAR(5),
    customer_segment  VARCHAR(5),       -- 'I'/'C' present in IND feed; NULL for others
    source_file_name  VARCHAR(255)      NOT NULL,
    feed_date         DATE              NOT NULL,
    ingested_at       TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (feed_date, country);

-- Semi-structured landing table for daily partner redemption JSON feed
CREATE OR REPLACE TABLE SKYPOINTS_DB.RAW.REDEMPTIONS_JSON_RAW (
    raw_payload       VARIANT           NOT NULL,
    source_file_name  VARCHAR(255)      NOT NULL,
    feed_date         DATE              NOT NULL,
    ingested_at       TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (feed_date);

-- =============================================================================
-- 2. STAGING LAYER (Silver)
-- Typed, trimmed, deduplicated records with derived Age & Stale_Member flag.
-- =============================================================================

CREATE OR REPLACE TABLE SKYPOINTS_DB.STAGING.MEMBERS_STAGING (
    member_id         VARCHAR(18)       NOT NULL,
    member_name       VARCHAR(255)      NOT NULL,
    enrollment_date   DATE              NOT NULL,
    last_flight_date  DATE,
    tier_code         VARCHAR(5),
    agent_name        VARCHAR(255),
    state             VARCHAR(10),
    country_code      VARCHAR(5)        NOT NULL,   -- Standardized ISO-3 (USA, IND, AUS, CAN, PHL)
    post_code         NUMBER(10, 0),
    dob               DATE,
    is_active         CHAR(1),
    customer_segment  CHAR(1),
    age               NUMBER(3, 0),                 -- Derived from DOB
    stale_member      BOOLEAN           NOT NULL,   -- TRUE when DATEDIFF('day', last_flight_date, CURRENT_DATE) > 90
    feed_date         DATE              NOT NULL,
    updated_at        TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_members_staging PRIMARY KEY (member_id, feed_date)
)
CLUSTER BY (country_code, feed_date);

-- Quarantine / Dead-Letter table for records failing validation checks
CREATE OR REPLACE TABLE SKYPOINTS_DB.STAGING.MEMBERS_QUARANTINE (
    member_id         VARCHAR(50),
    member_name       VARCHAR(255),
    raw_record        VARIANT,
    validation_errors VARCHAR(1000)     NOT NULL,
    source_file_name  VARCHAR(255),
    feed_date         DATE,
    quarantined_at    TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (feed_date);

-- Flattened, queryable redemption transactions (parsed via LATERAL FLATTEN)
CREATE OR REPLACE TABLE SKYPOINTS_DB.STAGING.REDEMPTIONS_FLAT (
    txn_id            VARCHAR(32)       NOT NULL,
    member_id         VARCHAR(18)       NOT NULL,
    feed_date         DATE              NOT NULL,
    txn_date          DATE              NOT NULL,
    partner           VARCHAR(100)      NOT NULL,
    miles_redeemed    NUMBER(12, 0)     NOT NULL,
    status            VARCHAR(20)       NOT NULL,   -- COMPLETED / PENDING / CANCELLED
    ingested_at       TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_redemptions_flat PRIMARY KEY (txn_id)
)
CLUSTER BY (txn_date, member_id);

-- =============================================================================
-- 3. COUNTRY-SPECIFIC TARGET TABLES (Gold / Mart)
-- Split per country as requested. Clustered by (member_id) for fast MERGE/DELETE
-- when a member moves countries ("latest record wins").
-- =============================================================================

CREATE OR REPLACE TABLE SKYPOINTS_DB.MART.TABLE_INDIA (
    member_id         VARCHAR(18)       NOT NULL,
    member_name       VARCHAR(255)      NOT NULL,
    enrollment_date   DATE              NOT NULL,
    last_flight_date  DATE,
    tier_code         VARCHAR(5),
    agent_name        VARCHAR(255),
    state             VARCHAR(10),
    country_code      VARCHAR(5)        DEFAULT 'IND',
    post_code         NUMBER(10, 0),
    dob               DATE,
    is_active         CHAR(1),
    customer_segment  CHAR(1),
    age               NUMBER(3, 0),
    stale_member      BOOLEAN           NOT NULL,
    last_feed_date    DATE              NOT NULL,
    updated_at        TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_table_india PRIMARY KEY (member_id)
)
CLUSTER BY (member_id);

CREATE OR REPLACE TABLE SKYPOINTS_DB.MART.TABLE_USA        LIKE SKYPOINTS_DB.MART.TABLE_INDIA;
CREATE OR REPLACE TABLE SKYPOINTS_DB.MART.TABLE_AUSTRALIA  LIKE SKYPOINTS_DB.MART.TABLE_INDIA;
CREATE OR REPLACE TABLE SKYPOINTS_DB.MART.TABLE_CANADA     LIKE SKYPOINTS_DB.MART.TABLE_INDIA;
CREATE OR REPLACE TABLE SKYPOINTS_DB.MART.TABLE_PHILIPPINES LIKE SKYPOINTS_DB.MART.TABLE_INDIA;

-- =============================================================================
-- 4. SNOWFLAKE SCALE PATTERN: FLATTENING JSON & JOINING TO MEMBER PROFILES
-- =============================================================================

-- Flatten semi-structured JSON into STAGING.REDEMPTIONS_FLAT (incremental MERGE)
/*
MERGE INTO SKYPOINTS_DB.STAGING.REDEMPTIONS_FLAT AS tgt
USING (
    SELECT
        r.value:txn_id::VARCHAR(32)            AS txn_id,
        raw_payload:member_id::VARCHAR(18)     AS member_id,
        TO_DATE(raw_payload:feed_date::STRING, 'YYYYMMDD') AS feed_date,
        TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD')      AS txn_date,
        r.value:partner::VARCHAR(100)          AS partner,
        r.value:miles_redeemed::NUMBER(12, 0)  AS miles_redeemed,
        r.value:status::VARCHAR(20)            AS status
    FROM SKYPOINTS_DB.RAW.REDEMPTIONS_JSON_RAW,
         LATERAL FLATTEN(input => raw_payload:redemptions) r
) AS src
ON tgt.txn_id = src.txn_id
WHEN MATCHED THEN UPDATE SET
    tgt.status         = src.status,
    tgt.miles_redeemed = src.miles_redeemed,
    tgt.feed_date      = src.feed_date
WHEN NOT MATCHED THEN INSERT (
    txn_id, member_id, feed_date, txn_date, partner, miles_redeemed, status
) VALUES (
    src.txn_id, src.member_id, src.feed_date, src.txn_date, src.partner, src.miles_redeemed, src.status
);
*/

-- Unified view across country tables for analytics & joining with redemptions
CREATE OR REPLACE VIEW SKYPOINTS_DB.MART.VW_ALL_MEMBERS AS
SELECT * FROM SKYPOINTS_DB.MART.TABLE_INDIA
UNION ALL SELECT * FROM SKYPOINTS_DB.MART.TABLE_USA
UNION ALL SELECT * FROM SKYPOINTS_DB.MART.TABLE_AUSTRALIA
UNION ALL SELECT * FROM SKYPOINTS_DB.MART.TABLE_CANADA
UNION ALL SELECT * FROM SKYPOINTS_DB.MART.TABLE_PHILIPPINES;

-- Analytics view: Member profile enriched with redemption totals
CREATE OR REPLACE VIEW SKYPOINTS_DB.MART.VW_MEMBER_REDEMPTION_SUMMARY AS
SELECT
    m.member_id,
    m.member_name,
    m.country_code,
    m.tier_code,
    m.age,
    m.stale_member,
    COUNT(r.txn_id)                                                     AS total_redemptions,
    COALESCE(SUM(CASE WHEN r.status = 'COMPLETED' THEN r.miles_redeemed ELSE 0 END), 0) AS completed_miles,
    COALESCE(SUM(CASE WHEN r.status = 'PENDING'   THEN r.miles_redeemed ELSE 0 END), 0) AS pending_miles,
    MAX(r.txn_date)                                                     AS last_redemption_date
FROM SKYPOINTS_DB.MART.VW_ALL_MEMBERS m
LEFT JOIN SKYPOINTS_DB.STAGING.REDEMPTIONS_FLAT r
    ON m.member_id = r.member_id
GROUP BY
    m.member_id, m.member_name, m.country_code, m.tier_code, m.age, m.stale_member;