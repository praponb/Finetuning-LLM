from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.39)
    .groupBy(
        "channel",
        F.when(F.col("status").isin(["SHIPPED", "COMPLETED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy("channel")
)
