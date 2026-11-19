from datetime import date
from pathlib import Path
from src.ingestion import read_flat_file
from src.redemptions import flatten_redemptions_json, join_members_with_redemptions
from src.routing import resolve_latest_members
from src.staging import transform_to_staging

DATA_DIR = Path("data/raw")


def test_flatten_redemptions_json_parses_sample_file(spark):
    df = flatten_redemptions_json(spark, DATA_DIR / "redemptions_20240115.json")
    rows = {r.txn_id: r for r in df.collect()}

    assert len(rows) == 2
    assert rows["RX10091"].member_id == "223457"
    assert rows["RX10091"].feed_date == date(2024, 1, 15)
    assert rows["RX10091"].txn_date == date(2024, 1, 10)
    assert rows["RX10091"].partner == "AeroLink"
    assert rows["RX10091"].miles_redeemed == 12000
    assert rows["RX10091"].status == "COMPLETED"
    assert rows["RX10092"].status == "PENDING"


def test_flatten_redemptions_deduplicates_txn_id_keeping_latest_status(spark, tmp_path):
    feed = tmp_path / "redemptions_updates.json"
    feed.write_text(
        """[
          {
            "member_id": "223457",
            "feed_date": "20240115",
            "redemptions": [
              {"txn_id": "RX10092", "txn_date": "20240113", "partner": "SkyPoints", "miles_redeemed": 5000, "status": "PENDING"}
            ]
          },
          {
            "member_id": "223457",
            "feed_date": "20240116",
            "redemptions": [
              {"txn_id": "RX10092", "txn_date": "20240113", "partner": "SkyPoints", "miles_redeemed": 5000, "status": "COMPLETED"}
            ]
          }
        ]"""
    )

    df = flatten_redemptions_json(spark, feed)
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0].txn_id == "RX10092"
    assert rows[0].status == "COMPLETED"
    assert rows[0].feed_date == date(2024, 1, 16)


def test_join_redemptions_to_member_profiles(spark):
    raw_members = read_flat_file(spark, DATA_DIR / "members_20240115.txt", feed_date="2024-01-15")
    members = resolve_latest_members(transform_to_staging(raw_members, as_of_date="2024-01-15"))
    redemptions = flatten_redemptions_json(spark, DATA_DIR / "redemptions_20240115.json")

    joined = {r.member_id: r for r in join_members_with_redemptions(members, redemptions).collect()}

    # Elena (223457) has 1 completed (12000) and 1 pending (5000) redemption
    assert joined["223457"].total_redemptions == 2
    assert joined["223457"].completed_miles == 12000
    assert joined["223457"].pending_miles == 5000
    assert joined["223457"].last_redemption_date == date(2024, 1, 13)

    # Ravi (223458) has no redemptions -> preserved via LEFT JOIN with 0s
    assert joined["223458"].total_redemptions == 0
    assert joined["223458"].completed_miles == 0
    assert joined["223458"].pending_miles == 0
    assert joined["223458"].last_redemption_date is None