from pyspark.sql import functions as F

src = df_web_events.filter(F.col("page").isNotNull())

duration_sec_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("duration_sec") >= 488, "high").when(F.col("duration_sec") >= 157, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"))
)

result = duration_sec_bucket_summary.filter(F.col("cart_count") > 3)
