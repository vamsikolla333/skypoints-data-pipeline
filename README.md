# SkyPoints Global Airline Loyalty Data Pipeline

End-to-end **Medallion Architecture** data pipeline built with **PySpark**, **Snowflake SQL**, and **Test-Driven Development (`pytest`)** for the SkyPoints airline loyalty program assessment.

## Architecture Overview

```text
Daily Flat File (|D|) + Country Excel Feeds (AUS/USA/IND) + Partner JSON Feed
                                   │
                                   ▼
                     ┌──────────────────────────┐
                     │   RAW / BRONZE LAYER     │  src/ingestion.py
                     │  (Schema harmonization)  │  sql/snowflake_ddl.sql
                     └────────────┬─────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │   DATA QUALITY ENGINE    │  src/validation.py
                     └───┬──────────────────┬───┘
                         │ Valid            │ Invalid (e.g. 2021-13-13)
                         ▼                  ▼
              ┌────────────────────┐  ┌────────────────────┐
              │  STAGING (SILVER)  │  │ QUARANTINE TABLE   │
              │ Age & Stale (>90d) │  │ (validation_errors)│
              └──────────┬─────────┘  └────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
┌───────────────────────┐    ┌──────────────────────────┐
│ COUNTRY MART TABLES   │    │ FLATTENED REDEMPTIONS    │
│ (Latest Record Wins)  │◄───│ (Exploded JSON + Dedup)  │
│ Table_India, USA, ... │    │ LEFT JOIN on member_id   │
└───────────────────────┘    └──────────────────────────┘