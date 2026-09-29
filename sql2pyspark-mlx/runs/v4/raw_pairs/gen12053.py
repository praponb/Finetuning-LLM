from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.amount") < 920)
    .groupBy(F.col("orders.channel"))
    .agg(
        F.sum(F.when(F.col("shipments.status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
        F.round(((100 * F.sum(F.when(F.col("shipments.status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .filter(F.col("delivered_count") > 63)
    .orderBy(F.col("delivered_count").desc())
)
