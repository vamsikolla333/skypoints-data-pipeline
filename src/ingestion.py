from datetime import date, datetime
from pathlib import Path
import openpyxl
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), True),
        StructField("member_name", StringType(), True),
        StructField("enrollment_date", StringType(), True),
        StructField("last_flight_date", StringType(), True),
        StructField("tier_code", StringType(), True),
        StructField("agent_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("feed_date", StringType(), True),
    ]
)


def _clean_col(col: F.Column) -> F.Column:
    trimmed = F.trim(col)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _yyyymmdd_to_iso(col: F.Column) -> F.Column:
    return F.when(
        F.length(col) == 8,
        F.concat_ws("-", F.substring(col, 1, 4), F.substring(col, 5, 2), F.substring(col, 7, 2)),
    ).otherwise(col)


def _ddmmyyyy_to_iso(col: F.Column) -> F.Column:
    padded = F.when(F.length(col) == 7, F.lpad(col, 8, "0")).otherwise(col)
    return F.when(
        F.length(padded) == 8,
        F.concat_ws(
            "-", F.substring(padded, 5, 4), F.substring(padded, 3, 2), F.substring(padded, 1, 2)
        ),
    ).otherwise(col)


def read_flat_file(spark: SparkSession, file_path: str | Path, feed_date: str) -> DataFrame:
    """Parse pipe-delimited SkyPoints member flat file into canonical raw DataFrame."""
    path = Path(file_path)
    lines = spark.read.text(str(path))
    detail_rows = lines.filter(F.trim(F.col("value")).startswith("|D|"))
    parts = F.split(F.col("value"), r"\|")

    # parts[0] is "" (before leading pipe), parts[1] is "D"
    return detail_rows.select(
        _clean_col(parts.getItem(3)).alias("member_id"),
        _clean_col(parts.getItem(2)).alias("member_name"),
        _yyyymmdd_to_iso(_clean_col(parts.getItem(4))).alias("enrollment_date"),
        _yyyymmdd_to_iso(_clean_col(parts.getItem(5))).alias("last_flight_date"),
        _clean_col(parts.getItem(6)).alias("tier_code"),
        _clean_col(parts.getItem(7)).alias("agent_name"),
        _clean_col(parts.getItem(8)).alias("state"),
        _clean_col(parts.getItem(9)).alias("country"),
        F.lit(None).cast("string").alias("post_code"),
        _ddmmyyyy_to_iso(_clean_col(parts.getItem(10))).alias("dob"),
        _clean_col(parts.getItem(11)).alias("is_active"),
        F.lit(None).cast("string").alias("customer_segment"),
        F.lit(path.name).alias("source_file"),
        F.lit(feed_date).alias("feed_date"),
    )


def _format_excel_date(value, is_us_int: bool = False) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    if is_us_int and isinstance(value, (int, float)):
        s = str(int(value)).zfill(8)  # e.g. 6152022 -> 06152022 (MMDDYYYY)
        return f"{s[4:8]}-{s[0:2]}-{s[2:4]}"
    text = str(value).strip()
    if not text or text.upper() == "NULL":
        return None
    return text


def _read_xlsx_records(path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h).strip() for h in next(rows_iter)]
    records = [dict(zip(headers, row)) for row in rows_iter if any(v is not None for v in row)]
    wb.close()
    return records


def read_country_excel(
    spark: SparkSession, file_path: str | Path, country_code: str, feed_date: str
) -> DataFrame:
    """Adapt country-specific Excel files (AUS, USA, IND) into the canonical raw schema."""
    path = Path(file_path)
    records = _read_xlsx_records(path)
    country = country_code.upper()
    rows = []

    for r in records:
        if country == "AUS":
            rows.append(
                (
                    f"AUS_{int(r['Unique ID'])}",
                    str(r["Member Name"]).strip(),
                    _format_excel_date(r["Date of Enrollment"]),
                    _format_excel_date(r["Date of Flight"]),
                    str(r["Tier Type"]).strip(),
                    None,
                    None,
                    "AUS",
                    None,
                    _format_excel_date(r["Date of Birth"]),
                    "A",
                    None,
                    path.name,
                    feed_date,
                )
            )
        elif country == "USA":
            rows.append(
                (
                    f"USA_{int(r['ID'])}",
                    str(r["Name"]).strip(),
                    _format_excel_date(r["EnrollmentDate"], is_us_int=True),
                    _format_excel_date(r["FlightDate"], is_us_int=True),
                    str(r["TierCode"]).strip(),
                    None,
                    None,
                    "USA",
                    None,
                    None,
                    "A",
                    None,
                    path.name,
                    feed_date,
                )
            )
        elif country == "IND":
            rows.append(
                (
                    f"IND_{int(r['ID'])}",
                    str(r["Name"]).strip(),
                    _format_excel_date(r["EnrollmentDate"]),
                    _format_excel_date(r["Flight Date"]),
                    str(r["TierCode"]).strip(),
                    None,
                    None,
                    "IND",
                    None,
                    _format_excel_date(r["DOB"]),
                    "A",
                    str(r["Individual or Corporate"]).strip(),
                    path.name,
                    feed_date,
                )
            )
        else:
            raise ValueError(f"Unsupported country Excel layout: {country_code}")

    return spark.createDataFrame(rows, schema=RAW_SCHEMA)