from src.ingestion import RAW_SCHEMA
from src.routing import resolve_latest_members, split_by_country
from src.staging import transform_to_staging


def _make_staged_df(spark, row_overrides_list):
    base = {
        "member_id": "223457",
        "member_name": "Elena",
        "enrollment_date": "2010-10-12",
        "last_flight_date": "2023-12-01",
        "tier_code": "GLD",
        "agent_name": "Sam",
        "state": "CA",
        "country": "USA",
        "post_code": None,
        "dob": "1985-05-03",
        "is_active": "A",
        "customer_segment": None,
        "source_file": "members.txt",
        "feed_date": "2024-01-15",
    }
    rows = []
    for overrides in row_overrides_list:
        rec = {**base, **overrides}
        rows.append(tuple(rec[f.name] for f in RAW_SCHEMA.fields))

    raw_df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    return transform_to_staging(raw_df, as_of_date="2024-02-15")


def test_latest_record_wins_when_member_moves_countries(spark):
    staged_df = _make_staged_df(
        spark,
        [
            {"member_id": "223457", "country": "USA", "state": "CA", "tier_code": "GLD", "feed_date": "2024-01-15"},
            {"member_id": "223457", "country": "IND", "state": "KA", "tier_code": "PLT", "feed_date": "2024-02-01"},
        ],
    )

    country_tables = split_by_country(staged_df)

    # Elena moved from USA -> IND: must appear ONLY in Table_India, never in Table_USA
    assert "Table_USA" not in country_tables
    assert "Table_India" in country_tables

    ind_rows = country_tables["Table_India"].collect()
    assert len(ind_rows) == 1
    assert ind_rows[0].member_id == "223457"
    assert ind_rows[0].state == "KA"
    assert ind_rows[0].tier_code == "PLT"


def test_split_by_country_routes_each_country_to_target_table(spark):
    staged_df = _make_staged_df(
        spark,
        [
            {"member_id": "1", "country": "IND"},
            {"member_id": "2", "country": "USA"},
            {"member_id": "3", "country": "AU"},
            {"member_id": "4", "country": "CAN"},
            {"member_id": "5", "country": "PHIL"},
        ],
    )

    country_tables = split_by_country(staged_df)

    assert set(country_tables.keys()) == {
        "Table_India",
        "Table_USA",
        "Table_Australia",
        "Table_Canada",
        "Table_Philippines",
    }
    assert sum(df.count() for df in country_tables.values()) == 5


def test_same_feed_date_tiebreaker_uses_latest_flight_date(spark):
    staged_df = _make_staged_df(
        spark,
        [
            {"member_id": "99", "country": "USA", "last_flight_date": "2024-01-01", "feed_date": "2024-01-15"},
            {"member_id": "99", "country": "CAN", "last_flight_date": "2024-01-12", "feed_date": "2024-01-15"},
        ],
    )

    latest = resolve_latest_members(staged_df).collect()
    assert len(latest) == 1
    assert latest[0].country_code == "CAN"