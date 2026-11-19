from pathlib import Path
from src.ingestion import read_flat_file, read_country_excel

DATA_DIR = Path("data/raw")


def test_read_flat_file_parses_and_trims_records(spark):
    df = read_flat_file(spark, DATA_DIR / "members_20240115.txt", feed_date="2024-01-15")

    assert df.count() == 5  # Header |H| row must be excluded

    elena = df.filter("member_id = '223457'").collect()[0]
    assert elena.member_name == "Elena"
    assert elena.enrollment_date == "2010-10-12"
    assert elena.last_flight_date == "2012-10-13"
    assert elena.tier_code == "GLD"
    assert elena.agent_name == "Sam"
    assert elena.state == "CA"
    assert elena.country == "USA"
    assert elena.dob == "1985-05-03"  # DDMMYYYY -> YYYY-MM-DD
    assert elena.is_active == "A"
    assert elena.post_code is None    # Omitted in flat file vs spec


def test_read_flat_file_handles_stripped_leading_zero_dob(spark, tmp_path):
    sample = tmp_path / "stripped_dob.txt"
    sample.write_text(
        "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active\n"
        "|D|Ravi|223458|20101012|20121013|SLV|Sam|TN|IND|3051985|A\n"
    )
    df = read_flat_file(spark, sample, feed_date="2024-01-15")
    row = df.collect()[0]
    assert row.dob == "1985-05-03"


def test_read_aus_excel_preserves_invalid_date_for_validation(spark):
    df = read_country_excel(spark, DATA_DIR / "AUS.xlsx", country_code="AUS", feed_date="2024-01-15")
    rows = {r.member_id: r for r in df.collect()}

    assert len(rows) == 3
    assert rows["AUS_1"].member_name == "Mike"
    assert rows["AUS_1"].dob is None  # NaT in AUS.xlsx
    assert rows["AUS_2"].enrollment_date == "2021-13-13"  # Preserved for Commit 4 validation


def test_read_usa_excel_normalizes_mmddyyyy_integers(spark):
    df = read_country_excel(spark, DATA_DIR / "USA.xlsx", country_code="USA", feed_date="2024-01-15")
    rows = {r.member_id: r for r in df.collect()}

    assert len(rows) == 3
    assert rows["USA_1"].enrollment_date == "2022-06-15"  # 6152022 -> 06/15/2022
    assert rows["USA_1"].last_flight_date == "2022-08-20"
    assert rows["USA_1"].dob is None  # USA feed has no DOB column


def test_read_ind_excel_captures_customer_segment(spark):
    df = read_country_excel(spark, DATA_DIR / "IND.xlsx", country_code="IND", feed_date="2024-01-15")
    rows = {r.member_id: r for r in df.collect()}

    assert len(rows) == 3
    assert rows["IND_1"].member_name == "Vikas"
    assert rows["IND_1"].customer_segment == "I"
    assert rows["IND_2"].customer_segment == "C"