from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["mobile", "partner"]), "priority").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("discount")).otherwise(0)).alias("shipped_discount"),
        F.count("*").alias("num_orders"),
    )
    .filter(F.col("shipped_discount") > 61)
    .select("channel_group", "shipped_discount", "num_orders")
)
