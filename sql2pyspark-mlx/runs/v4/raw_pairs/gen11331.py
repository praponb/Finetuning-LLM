from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.max("order_date").alias("latest_order_date"),
        F.countDistinct("discount").alias("unique_stores"),
    )
    .orderBy("unique_stores", F.col("web_count").desc(), "channel")
    .limit(10)
)
