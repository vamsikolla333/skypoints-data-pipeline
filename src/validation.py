from pyspark.sql import DataFrame, Window, functions as F

VALID_TIERS = ["GLD", "SLV", "PLT"]
VALID_ACTIVE_FLAGS = ["A", "I"]


def validate_members(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Validate raw member records and split them into (valid_df, quarantine_df).
    Bad rows are never silently dropped; all failed rules are recorded in `validation_errors`.
    """
    parsed = (
        df.withColumn("_enroll_dt", F.to_date(F.col("enrollment_date"), "yyyy-MM-dd"))
        .withColumn("_flight_dt", F.to_date(F.col("last_flight_date"), "yyyy-MM-dd"))
        .withColumn("_dob_dt", F.to_date(F.col("dob"), "yyyy-MM-dd"))
    )

    # Flag duplicate (member_id, feed_date) occurrences within the same batch
    dup_window = Window.partitionBy("member_id", "feed_date").orderBy(F.monotonically_increasing_id())
    parsed = parsed.withColumn(
        "_dup_rn",
        F.when(F.col("member_id").isNotNull(), F.row_number().over(dup_window)).otherwise(F.lit(1)),
    )

    error_checks = [
        F.when(F.col("member_id").isNull(), F.lit("MISSING_MEMBER_ID")),
        F.when(F.col("member_name").isNull(), F.lit("MISSING_MEMBER_NAME")),
        F.when(F.col("enrollment_date").isNull(), F.lit("MISSING_ENROLLMENT_DATE")),
        F.when(F.col("country").isNull(), F.lit("MISSING_COUNTRY")),
        F.when(F.length(F.col("member_id")) > 18, F.lit("MEMBER_ID_EXCEEDS_MAX_LENGTH")),
        F.when(F.col("_dup_rn") > 1, F.lit("DUPLICATE_MEMBER_ID")),
        F.when(
            F.col("enrollment_date").isNotNull() & F.col("_enroll_dt").isNull(),
            F.lit("INVALID_ENROLLMENT_DATE"),
        ),
        F.when(
            F.col("last_flight_date").isNotNull() & F.col("_flight_dt").isNull(),
            F.lit("INVALID_FLIGHT_DATE"),
        ),
        F.when(
            F.col("dob").isNotNull() & F.col("_dob_dt").isNull(),
            F.lit("INVALID_DOB"),
        ),
        F.when(
            F.col("_flight_dt").isNotNull()
            & F.col("_enroll_dt").isNotNull()
            & (F.col("_flight_dt") < F.col("_enroll_dt")),
            F.lit("FLIGHT_BEFORE_ENROLLMENT"),
        ),
        F.when(
            F.col("tier_code").isNotNull() & ~F.col("tier_code").isin(VALID_TIERS),
            F.lit("INVALID_TIER_CODE"),
        ),
        F.when(
            F.col("is_active").isNotNull() & ~F.col("is_active").isin(VALID_ACTIVE_FLAGS),
            F.lit("INVALID_ACTIVE_FLAG"),
        ),
    ]

    checked = parsed.withColumn(
        "validation_errors",
        F.concat_ws(", ", F.array(*error_checks)),
    )

    helper_cols = ["_enroll_dt", "_flight_dt", "_dob_dt", "_dup_rn"]
    valid_df = checked.filter(F.col("validation_errors") == "").drop(*helper_cols, "validation_errors")
    quarantine_df = checked.filter(F.col("validation_errors") != "").drop(*helper_cols)

    return valid_df, quarantine_df