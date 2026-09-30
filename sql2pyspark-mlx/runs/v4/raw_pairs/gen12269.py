from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(~F.col("orders.status").isin(["RETURNED", "COMPLETED", "PENDING"]))
    .groupBy(F.col("stores.id"), F.col("stores.city"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.round(((100 * F.sum(F.when(F.col("orders.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)
