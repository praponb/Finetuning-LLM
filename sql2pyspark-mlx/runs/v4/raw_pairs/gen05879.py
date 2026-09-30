from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("country").orderBy("age", "id")
w_running = Window.partitionBy("country", "segment").orderBy("credit_limit", "id").rowsBetween(-6, -1)

scored = (
    df_customers
    .withColumn("credit_limit_bucket", F.ntile(4).over(w))
    .withColumn("age_bucket", F.ntile(3).over(w2))
    .withColumn("credit_limit_avg_6_prior", F.avg("credit_limit").over(w_running))
)

result = (
    scored
    .groupBy(
        "segment",
        F.when(F.col("age_bucket") >= 8, "LARGE").otherwise("SMALL").alias("age_bucket_tier"),
        F.when(F.col("credit_limit_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("credit_limit_bucket_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.stddev_samp("age").alias("age_stddev"),
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.max("age").alias("max_age"),
    )
)
