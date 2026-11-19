from pathlib import Path
from pyspark.sql import DataFrame
from src.ingestion import read_country_excel, read_flat_file
from src.redemptions import flatten_redemptions_json, join_members_with_redemptions
from src.routing import resolve_latest_members, split_by_country
from src.spark_session import get_spark_session
from src.staging import transform_to_staging
from src.validation import validate_members

DATA_DIR = Path("data/raw")


def run_pipeline(spark, data_dir: Path = DATA_DIR, feed_date: str = "2024-01-15") -> dict:
    """Execute the end-to-end SkyPoints Medallion pipeline across all sample feeds."""
    # 1. Raw / Bronze Ingestion
    flat_df = read_flat_file(spark, data_dir / "members_20240115.txt", feed_date=feed_date)
    aus_df = read_country_excel(spark, data_dir / "AUS.xlsx", country_code="AUS", feed_date=feed_date)
    usa_df = read_country_excel(spark, data_dir / "USA.xlsx", country_code="USA", feed_date=feed_date)
    ind_df = read_country_excel(spark, data_dir / "IND.xlsx", country_code="IND", feed_date=feed_date)

    raw_members_df: DataFrame = (
        flat_df.unionByName(aus_df).unionByName(usa_df).unionByName(ind_df)
    )

    # 2. Validation & Quarantine Routing
    valid_df, quarantine_df = validate_members(raw_members_df)

    # 3. Staging / Silver Layer (derived Age & Stale_Member > 90 days)
    staging_df = transform_to_staging(valid_df, as_of_date=feed_date)

    # 4. Gold Country-Specific Target Tables (latest record wins)
    latest_members_df = resolve_latest_members(staging_df)
    country_tables = split_by_country(staging_df)

    # 5. Semi-Structured Partner Redemption JSON Feed & Member Join
    redemptions_flat_df = flatten_redemptions_json(spark, data_dir / "redemptions_20240115.json")
    member_redemption_summary_df = join_members_with_redemptions(
        latest_members_df, redemptions_flat_df
    )

    return {
        "raw": raw_members_df,
        "valid": valid_df,
        "quarantine": quarantine_df,
        "staging": staging_df,
        "country_tables": country_tables,
        "redemptions_flat": redemptions_flat_df,
        "member_redemption_summary": member_redemption_summary_df,
    }


def main() -> None:
    spark = get_spark_session("SkyPointsLiveDemo")
    try:
        results = run_pipeline(spark)

        print("\n=== 1. QUARANTINED RECORDS (DATA QUALITY FAILURES) ===")
        results["quarantine"].select(
            "member_id", "member_name", "enrollment_date", "source_file", "validation_errors"
        ).show(truncate=False)

        print("\n=== 2. STAGING TABLE (WITH DERIVED AGE & STALE_MEMBER > 90D) ===")
        results["staging"].select(
            "member_id",
            "member_name",
            "country_code",
            "tier_code",
            "dob",
            "age",
            "last_flight_date",
            "stale_member",
        ).orderBy("country_code", "member_id").show(truncate=False)

        print("\n=== 3. COUNTRY-SPECIFIC TARGET TABLES ===")
        for table_name, df in results["country_tables"].items():
            print(f"\n--- {table_name} ({df.count()} rows) ---")
            df.select(
                "member_id", "member_name", "tier_code", "age", "stale_member", "customer_segment"
            ).show(truncate=False)

        print("\n=== 4. FLATTENED PARTNER REDEMPTIONS (JSON) ===")
        results["redemptions_flat"].show(truncate=False)

        print("\n=== 5. MEMBER PROFILE + REDEMPTION SUMMARY (LEFT JOIN) ===")
        results["member_redemption_summary"].select(
            "member_id",
            "member_name",
            "country_code",
            "tier_code",
            "total_redemptions",
            "completed_miles",
            "pending_miles",
            "last_redemption_date",
        ).orderBy("total_redemptions", ascending=False).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()