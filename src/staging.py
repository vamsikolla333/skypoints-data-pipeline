from pyspark.sql import DataFrame, functions as F

COUNTRY_CODE_MAP = {
    "AU": "AUS",
    "AUS": "AUS",
    "AUSTRALIA": "AUS",
    "US": "USA",
    "USA": "USA",
    "IN": "IND",
    "IND": "IND",
    "INDIA": "IND",
    "PHIL": "PHL",
    "PHL": "PHL",
    "CAN": "CAN",
    "CANADA": "CAN",
}


def _normalize_country_col(col: F.Column) -> F.Column:
    upper_col = F.upper(F.trim(col))
    mapping_expr = F.create_map([F.lit(x) for pair in COUNTRY_CODE_MAP.items() for x in pair])
    return F.coalesce(mapping_expr[upper_col], upper_col)


def transform_to_staging(df: DataFrame, as_of_date: str | None = None) -> DataFrame:
    """
    Load validated records into the Staging (Silver) layer:
    - Cast date fields to DATE type
    - Normalize country codes to ISO-3 (`country_code`)
    - Derive `age` from `dob` (NULL-safe)
    - Derive `stale_member` flag where days since `last_flight_date` > 90
    """
    ref_date = F.to_date(F.lit(as_of_date), "yyyy-MM-dd") if as_of_date else F.current_date()

    typed = (
        df.withColumn("enrollment_date", F.to_date(F.col("enrollment_date"), "yyyy-MM-dd"))
        .withColumn("last_flight_date", F.to_date(F.col("last_flight_date"), "yyyy-MM-dd"))
        .withColumn("dob", F.to_date(F.col("dob"), "yyyy-MM-dd"))
        .withColumn("feed_date", F.to_date(F.col("feed_date"), "yyyy-MM-dd"))
        .withColumn("country_code", _normalize_country_col(F.col("country")))
        .drop("country")
    )

    return typed.withColumn(
        "age",
        F.when(
            F.col("dob").isNotNull(),
            F.floor(F.months_between(ref_date, F.col("dob")) / F.lit(12)).cast("int"),
        ).otherwise(F.lit(None).cast("int")),
    ).withColumn(
        "stale_member",
        F.when(F.col("last_flight_date").isNull(), F.lit(True)).otherwise(
            F.datediff(ref_date, F.col("last_flight_date")) > F.lit(90)
        ),
    )