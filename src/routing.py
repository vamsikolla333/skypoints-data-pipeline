from pyspark.sql import DataFrame, Window, functions as F

COUNTRY_TABLE_MAP = {
    "IND": "Table_India",
    "USA": "Table_USA",
    "AUS": "Table_Australia",
    "CAN": "Table_Canada",
    "PHL": "Table_Philippines",
}


def resolve_latest_members(staging_df: DataFrame) -> DataFrame:
    """
    Apply 'latest record wins' per member_id so a member who moves countries
    is kept only under their most recent record.
    """
    latest_window = Window.partitionBy("member_id").orderBy(
        F.col("feed_date").desc_nulls_last(),
        F.col("last_flight_date").desc_nulls_last(),
        F.col("enrollment_date").desc_nulls_last(),
    )

    return (
        staging_df.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def split_by_country(staging_df: DataFrame) -> dict[str, DataFrame]:
    """
    Resolve latest member state and split into per-country target DataFrames
    (e.g., Table_India, Table_USA, Table_Australia).
    """
    latest_df = resolve_latest_members(staging_df).cache()
    countries = [
        row["country_code"]
        for row in latest_df.select("country_code").distinct().collect()
        if row["country_code"] is not None
    ]

    country_tables: dict[str, DataFrame] = {}
    for code in sorted(countries):
        table_name = COUNTRY_TABLE_MAP.get(code, f"Table_{code}")
        country_tables[table_name] = latest_df.filter(F.col("country_code") == F.lit(code))

    return country_tables