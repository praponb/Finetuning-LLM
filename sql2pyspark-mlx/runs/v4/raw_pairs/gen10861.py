from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"))
    .filter(F.col("web_count") > 60)
    .select("order_month", "web_count")
    .orderBy(F.col("web_count").desc(), F.col("order_month").desc())
)
