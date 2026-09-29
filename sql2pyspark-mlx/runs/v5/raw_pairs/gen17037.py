from pyspark.sql import functions as F

remaining_keys = (
    df_store_sales
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([1, 4]))
        .select("id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("revenue") >= 4200, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        "store_id",
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("event_count"),
        F.bit_and("units").alias("units_bit_and"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("units_bit_and").desc_nulls_last())
)
