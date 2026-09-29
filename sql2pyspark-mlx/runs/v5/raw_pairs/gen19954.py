from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 8]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .groupingSets(
        [["region", "city"], ["region"]],
        F.col("d.region").alias("region"),
        F.col("d.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.revenue")).alias("max_revenue"),
        F.max(F.col("a.units")).alias("max_units"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("city").asc_nulls_last())
)
