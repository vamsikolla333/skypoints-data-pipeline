from pathlib import Path
from src.ingestion import RAW_SCHEMA, read_country_excel
from src.validation import validate_members

DATA_DIR = Path("data/raw")


def _make_row(**overrides):
    base = {
        "member_id": "1001",
        "member_name": "Elena",
        "enrollment_date": "2020-01-10",
        "last_flight_date": "2022-05-15",
        "tier_code": "GLD",
        "agent_name": "Sam",
        "state": "CA",
        "country": "USA",
        "post_code": None,
        "dob": "1985-05-03",
        "is_active": "A",
        "customer_segment": None,
        "source_file": "test.txt",
        "feed_date": "2024-01-15",
    }
    base.update(overrides)
    return tuple(base[f.name] for f in RAW_SCHEMA.fields)


def test_valid_record_passes_validation(spark):
    df = spark.createDataFrame([_make_row()], schema=RAW_SCHEMA)
    valid_df, quarantine_df = validate_members(df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 0


def test_mandatory_fields_quarantined(spark):
    df = spark.createDataFrame(
        [_make_row(member_id=None, member_name=None)],
        schema=RAW_SCHEMA,
    )
    valid_df, quarantine_df = validate_members(df)

    assert valid_df.count() == 0
    errors = quarantine_df.collect()[0].validation_errors
    assert "MISSING_MEMBER_ID" in errors
    assert "MISSING_MEMBER_NAME" in errors


def test_duplicate_member_id_in_same_batch_quarantined(spark):
    df = spark.createDataFrame(
        [_make_row(member_id="223457"), _make_row(member_id="223457")],
        schema=RAW_SCHEMA,
    )
    valid_df, quarantine_df = validate_members(df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 1
    assert "DUPLICATE_MEMBER_ID" in quarantine_df.collect()[0].validation_errors


def test_invalid_date_from_aus_excel_is_quarantined(spark):
    aus_df = read_country_excel(spark, DATA_DIR / "AUS.xlsx", country_code="AUS", feed_date="2024-01-15")
    valid_df, quarantine_df = validate_members(aus_df)

    # Mike (AUS_1, null DOB is allowed) and Cristina (AUS_3) pass; Jonnathan (AUS_2, 2021-13-13) is quarantined
    assert valid_df.count() == 2
    assert quarantine_df.count() == 1
    bad_row = quarantine_df.collect()[0]
    assert bad_row.member_id == "AUS_2"
    assert "INVALID_ENROLLMENT_DATE" in bad_row.validation_errors


def test_flight_before_enrollment_and_bad_tier_quarantined(spark):
    df = spark.createDataFrame(
        [_make_row(enrollment_date="2022-05-01", last_flight_date="2021-01-01", tier_code="BRZ")],
        schema=RAW_SCHEMA,
    )
    valid_df, quarantine_df = validate_members(df)

    assert valid_df.count() == 0
    errors = quarantine_df.collect()[0].validation_errors
    assert "FLIGHT_BEFORE_ENROLLMENT" in errors
    assert "INVALID_TIER_CODE" in errors