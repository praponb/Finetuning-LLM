from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(~((F.col("cost") >= 83) & ((F.col("status") != "DELIVERED") | F.col("carrier").isin(["FedEx", "Kerry", "DHL"]))))
)

shipments_stats = (
    base
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
)

result = (
    shipments_stats
    .filter(F.col("pct_lost") >= 72)
    .orderBy(F.col("order_id").desc(), F.col("pct_lost").desc())
)
