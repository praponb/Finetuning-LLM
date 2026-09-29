from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("is_flagged_plus_status_code").desc_nulls_last(), "id", "key")
w_part = Window.partitionBy("key").orderBy("is_flagged", "id", "key")
w3 = Window.partitionBy("key").orderBy("is_flagged", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("is_flagged_plus_status_code", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    parsed
    .filter(F.col("status_code_high").between(1, 2))
    .withColumn("key_group", F.when(F.col("key").isin(["trace", "region"]), "Group A").otherwise("Other"))
    .withColumn("is_flagged_plus_status_code", (F.col("is_flagged") + F.col("status_code")))
    .withColumn("key_clean", F.coalesce("key", F.lit("UNKNOWN")))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "key", "is_flagged", "is_flagged_plus_status_code", "key_clean")
)

enriched = (
    deduped
    .select(
        "id",
        "key",
        "is_flagged",
        "is_flagged_plus_status_code",
        F.row_number().over(w_part).alias("seq_no"),
        F.last("key", ignorenulls=True).over(w3).alias("last_known_key"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "key", "is_flagged", "is_flagged_plus_status_code", "last_known_key")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("is_flagged_plus_status_code") >= 68, "LARGE").otherwise("SMALL").alias("is_flagged_plus_status_code_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.percentile_approx("is_flagged_plus_status_code", 0.95).alias("is_flagged_plus_status_code_p95"),
        F.avg("is_flagged").alias("avg_is_flagged"),
        F.max(F.when(F.col("key") == "region", F.col("is_flagged"))).alias("region_max_is_flagged"),
        F.countDistinct("key").alias("distinct_keys"),
    )
    .orderBy("cnt")
)
