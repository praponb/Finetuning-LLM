from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(126, 311))
    .groupBy(
        F.when(F.col("duration_sec") >= 446, "high").when(F.col("duration_sec") >= 142, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
    .filter(F.col("pct_checkout") < 69)
    .orderBy("duration_sec_bucket")
)
