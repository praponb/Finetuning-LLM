from pyspark.sql import functions as F

src = df_orders.filter(F.col("store_id").isNull())

summary = (
    src
    .groupBy(
        F.when(F.col("amount") >= 1280, "high").when(F.col("amount") >= 500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"))
)

result = (
    summary
    .filter(F.col("web_count") > 62)
    .orderBy("web_count", "amount_bucket")
)
