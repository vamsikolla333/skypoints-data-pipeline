from datetime import date
from src.ingestion import RAW_SCHEMA
from src.staging import transform_to_staging


def _make_valid_row(**overrides):
    base = {
        "member_id": "223457",
        "member_name": "Elena",
        "enrollment_date": "2010-10-12",
        "last_flight_date": "2023-10-01",
        "tier_code": "GLD",
        "agent_name": "Sam",
        "state": "CA",
        "country": "USA",
        "post_code": None,
        "dob": "1985-05-03",
        "is_active": "A",
        "customer_segment": None,
        "source_file": "members_20240115.txt",
        "feed_date": "2024-01-15",
    }
    base.update(overrides)
    return tuple(base[f.name] for f in RAW_SCHEMA.fields)


def test_staging_computes_age_and_handles_null_dob(spark):
    rows = [
        _make_valid_row(member_id="1", dob="1985-05-03"),  # 38 on 2024-01-15
        _make_valid_row(member_id="2", dob="1985-01-15"),  # 39 on birthday 2024-01-15
        _make_valid_row(member_id="3", dob=None),          # NULL DOB -> NULL age
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    staged = {r.member_id: r for r in transform_to_staging(df, as_of_date="2024-01-15").collect()}

    assert staged["1"].age == 38
    assert staged["2"].age == 39
    assert staged["3"].age is None
    assert staged["1"].dob == date(1985, 5, 3)


def test_staging_computes_stale_member_flag_at_90_day_boundary(spark):
    # Reference date: 2024-01-15
    # 90 days prior = 2023-10-17 (NOT stale), 91 days prior = 2023-10-16 (STALE)
    rows = [
        _make_valid_row(member_id="fresh", last_flight_date="2024-01-01"),
        _make_valid_row(member_id="exact_90", last_flight_date="2023-10-17"),
        _make_valid_row(member_id="over_90", last_flight_date="2023-10-16"),
        _make_valid_row(member_id="never_flew", last_flight_date=None),
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    staged = {r.member_id: r for r in transform_to_staging(df, as_of_date="2024-01-15").collect()}

    assert staged["fresh"].stale_member is False
    assert staged["exact_90"].stale_member is False
    assert staged["over_90"].stale_member is True
    assert staged["never_flew"].stale_member is True


def test_staging_normalizes_country_codes_to_iso3(spark):
    rows = [
        _make_valid_row(member_id="1", country="AU"),
        _make_valid_row(member_id="2", country="PHIL"),
        _make_valid_row(member_id="3", country="ind"),
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    staged = {r.member_id: r for r in transform_to_staging(df, as_of_date="2024-01-15").collect()}

    assert staged["1"].country_code == "AUS"
    assert staged["2"].country_code == "PHL"
    assert staged["3"].country_code == "IND"