from pyspark.sql import functions as F

matched_keys = (
    df_store_sales
    .select("store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("revenue") > 1200)
        .select("store_id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("revenue") >= 3400, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.sum("revenue").alias("total_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc_nulls_last())
)
