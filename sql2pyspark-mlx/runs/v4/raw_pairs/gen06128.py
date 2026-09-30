from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-10-01").cast("date"))
    .select(
        "*",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
        F.round((F.col("units") * 1.1), 2).alias("units_scaled"),
    )
)

matched_keys = (
    valid
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") != 35)
        .select("product_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("sale_date") > F.lit("2024-03-13").cast("date"))
    .withColumn("revenue_per_unit_rounded", F.round("revenue_per_unit", 1))
)

daily = (
    base
    .groupBy("store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    daily
    .select("store_id", "row_count", "num_rows")
    .orderBy(F.col("row_count").desc_nulls_first())
)
