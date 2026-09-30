from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 361, "high").when(F.col("duration_sec") >= 146, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.when(F.col("page").isin(["/cart", "/checkout"]), "tier_1").otherwise("other").alias("page_group"),
    )
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .orderBy(F.col("avg_duration_sec").desc(), F.col("page_group").desc())
)
