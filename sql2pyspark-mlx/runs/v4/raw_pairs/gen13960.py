from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code_high")
w2 = Window.partitionBy("attr_key")
w3 = Window.orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("status_code_high_cume_dist", "id", "attr_key")
w_grp = Window.partitionBy("attr_key").orderBy("flag_bit2", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high_cume_dist").desc(), "id", "attr_key")
w7 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high_cume_dist_bucket_2").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

scored = (
    parsed
    .select(
        "*",
        F.cume_dist().over(w).alias("status_code_high_cume_dist"),
        F.count("status_code_high").over(w2).alias("attr_key_count_status_code_high"),
        F.sum("status_code_high").over(w3).alias("running_status_code_high"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.ntile(3).over(w4).alias("status_code_high_cume_dist_bucket"),
        F.round((F.col("attr_key_count_status_code_high") / F.sum("attr_key_count_status_code_high").over(w2)), 4).alias("attr_key_count_status_code_high_share"),
        F.lag("status_code_high_cume_dist", 2).over(w_grp).alias("prev_status_code_high_cume_dist"),
        F.ntile(4).over(w6).alias("status_code_high_cume_dist_bucket_2"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .cube(
        F.when(F.col("status_code_high_cume_dist_bucket_2") >= 8, "HIGH").otherwise("LOW").alias("status_code_high_cume_dist_bucket_2_tier"),
        F.when(F.col("status_code_high_cume_dist_bucket") >= 7, "LARGE").otherwise("SMALL").alias("status_code_high_cume_dist_bucket_tier"),
        F.when(F.col("attr_key_count_status_code_high") >= 15, "HIGH").otherwise("LOW").alias("attr_key_count_status_code_high_tier"),
    )
    .agg(
        F.percentile_approx("attr_key_count_status_code_high", 0.9).alias("attr_key_count_status_code_high_p90"),
        F.count("*").alias("event_count"),
        F.percentile_approx("status_code_high_cume_dist", [0.25, 0.5, 0.75]).alias("status_code_high_cume_dist_quartiles"),
        F.round(F.sum("status_code_high_cume_dist") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("status_code_high_cume_dist_per_region"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys_2"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
