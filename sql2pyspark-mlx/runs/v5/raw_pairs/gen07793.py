from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "left")
    .filter(F.col("store_summary.pct_cancelled") > 33)
    .select(
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
        F.col("store_summary.pct_shipped"),
        F.col("store_summary.pct_cancelled"),
    )
)
