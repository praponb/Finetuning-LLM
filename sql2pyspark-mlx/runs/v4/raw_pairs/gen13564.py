from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.sum("discount").alias("total_discount"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount")).otherwise(0)).alias("partner_discount"),
    )
    .orderBy(F.col("web_rows").desc())
)
