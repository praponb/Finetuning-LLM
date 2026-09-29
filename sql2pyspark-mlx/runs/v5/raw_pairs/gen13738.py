from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["SHIPPED", "PENDING"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .filter(F.col("shipped_count") >= 72)
    .select("status_group", "shipped_count")
    .orderBy("shipped_count", "status_group")
)
