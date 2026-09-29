from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("is_flagged").desc_nulls_last())
w2 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc(), "id", "key")
w3 = Window.partitionBy("service").orderBy(F.col("is_flagged").desc_nulls_last())
w4 = Window.partitionBy("host").orderBy(F.col("is_flagged").asc_nulls_first())
w_part = Window.orderBy("latest_rank", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

latest = (
    unpacked
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .withColumn("status_code_high_bucket", F.ntile(4).over(w2))
    .withColumn("is_flagged_dense_rank", F.dense_rank().over(w3))
    .withColumn("is_flagged_cume_dist", F.cume_dist().over(w4))
    .withColumn("next_status_code", F.lead("status_code", 1).over(w_part))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("is_flagged_cume_dist") >= 0.74, "LARGE").otherwise("SMALL").alias("is_flagged_cume_dist_tier"),
        F.when(F.col("is_flagged") >= 1, "HIGH").otherwise("LOW").alias("is_flagged_tier"),
    )
    .agg(
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.sum(F.when(F.col("service") == "auth", F.col("status_code_high")).otherwise(0)).alias("auth_status_code_high"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.max("next_status_code").alias("max_next_status_code"),
    )
    .orderBy("is_flagged_cume_dist_tier")
)
