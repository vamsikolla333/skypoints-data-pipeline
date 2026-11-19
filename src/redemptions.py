from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

REDEMPTION_FEED_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), True),
        StructField("feed_date", StringType(), True),
        StructField(
            "redemptions",
            ArrayType(
                StructType(
                    [
                        StructField("txn_id", StringType(), True),
                        StructField("txn_date", StringType(), True),
                        StructField("partner", StringType(), True),
                        StructField("miles_redeemed", LongType(), True),
                        StructField("status", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


def flatten_redemptions_json(spark: SparkSession, file_path: str | Path) -> DataFrame:
    """
    Parse semi-structured partner redemption JSON feed into a flattened, typed DataFrame.
    Deduplicates by txn_id keeping the latest feed_date (handles PENDING -> COMPLETED updates).
    """
    raw_df = spark.read.option("multiLine", "true").schema(REDEMPTION_FEED_SCHEMA).json(str(file_path))

    exploded = raw_df.select(
        F.trim(F.col("member_id")).alias("member_id"),
        F.to_date(F.trim(F.col("feed_date")), "yyyyMMdd").alias("feed_date"),
        F.explode(F.col("redemptions")).alias("r"),
    )

    flat = exploded.select(
        F.trim(F.col("r.txn_id")).alias("txn_id"),
        F.col("member_id"),
        F.col("feed_date"),
        F.to_date(F.trim(F.col("r.txn_date")), "yyyyMMdd").alias("txn_date"),
        F.trim(F.col("r.partner")).alias("partner"),
        F.col("r.miles_redeemed").cast("long").alias("miles_redeemed"),
        F.upper(F.trim(F.col("r.status"))).alias("status"),
    )

    latest_txn = Window.partitionBy("txn_id").orderBy(F.col("feed_date").desc_nulls_last())
    return (
        flat.withColumn("_rn", F.row_number().over(latest_txn))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def join_members_with_redemptions(members_df: DataFrame, redemptions_df: DataFrame) -> DataFrame:
    """
    Aggregate flattened redemptions by member_id and LEFT JOIN back to member profiles.
    """
    agg_redemptions = redemptions_df.groupBy("member_id").agg(
        F.count("txn_id").alias("total_redemptions"),
        F.sum(
            F.when(F.col("status") == "COMPLETED", F.col("miles_redeemed")).otherwise(F.lit(0))
        ).alias("completed_miles"),
        F.sum(
            F.when(F.col("status") == "PENDING", F.col("miles_redeemed")).otherwise(F.lit(0))
        ).alias("pending_miles"),
        F.max("txn_date").alias("last_redemption_date"),
    )

    return (
        members_df.join(agg_redemptions, on="member_id", how="left")
        .withColumn("total_redemptions", F.coalesce(F.col("total_redemptions"), F.lit(0)))
        .withColumn("completed_miles", F.coalesce(F.col("completed_miles"), F.lit(0)))
        .withColumn("pending_miles", F.coalesce(F.col("pending_miles"), F.lit(0)))
    )