from pyspark.sql import functions as F

matched_keys = (
    df_store_sales
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") > 38)
        .select("store_id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup(
        "product_id",
        F.when(F.col("units") >= 40, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("units"), 2).alias("avg_units_2"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
    .filter(F.col("avg_units") >= 0)
)
