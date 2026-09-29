from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("search_count").desc())

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "context",
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

result = (
    parsed
    .groupBy(
        "service",
        F.when(F.col("flag_bit2") >= 1, "PREMIUM").otherwise("STANDARD").alias("flag_bit2_tier"),
        "level",
    )
    .agg(
        F.percentile_approx("flag_bit2", 0.95).alias("flag_bit2_p95"),
        F.sum(F.when(F.col("service") == "billing", F.col("flag_bit2"))).alias("billing_total_flag_bit2"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.count_if(F.col("service") == "billing").alias("billing_events"),
        F.max(F.when(F.col("level") == "INFO", F.col("flag_bit2"))).alias("info_max_flag_bit2"),
    )
    .filter(F.col("billing_events") > 1)
    .withColumn("search_count_percent_rank", F.percent_rank().over(w))
)
