from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("carrier").isin(["FedEx", "Kerry"]) & (F.col("status") == "DELIVERED"))
)

summary = (
    recent_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
)

result = summary.select("order_id", "delivered_count")
