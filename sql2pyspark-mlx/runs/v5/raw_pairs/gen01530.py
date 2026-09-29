from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "RETURNED"]) | F.col("order_id").isin([9, 27, 30, 33]))
    .groupBy(
        F.when(F.col("cost") >= 101, "high").when(F.col("cost") >= 35, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy("avg_cost", "cost_bucket")
)
