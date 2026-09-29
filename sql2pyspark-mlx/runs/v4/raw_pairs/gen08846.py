from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("attr_key").orderBy("status_code_high_times_flag_bit2")
w_hist = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key")
w_part = Window.partitionBy("attr_key").orderBy("status_code_high_times_flag_bit2", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy("status_code_high_times_flag_bit2", "id", "attr_key").rowsBetween(-2, -1)
w5 = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("attr_key").orderBy("status_code_high_times_flag_bit2_cume_dist", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

parsed = (
    lines
    .filter(F.col("attr_key") == "version")
    .select(
        "id",
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    parsed
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 4))
    .withColumn("status_code_high_times_flag_bit2", (F.col("status_code_high") * F.col("flag_bit2")))
)

metrics = (
    filtered
    .withColumn("status_code_high_times_flag_bit2_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("next_status_code_high_times_flag_bit2", F.lead("status_code_high_times_flag_bit2", 1).over(w_hist))
    .withColumn("prev_status_code_high", F.lag("status_code_high", 2, 0).over(w_hist))
    .withColumn("status_code_high_avg_6", F.avg("status_code_high").over(w_part))
    .withColumn("status_code_high_times_flag_bit2_avg_2_prior", F.avg("status_code_high_times_flag_bit2").over(w4))
)

base = (
    metrics
    .filter(F.col("status_code_high_times_flag_bit2_cume_dist") >= 0.54)
    .withColumn("status_code_high_times_flag_bit2_avg_2_prior_rounded", F.round("status_code_high_times_flag_bit2_avg_2_prior", 1))
)

metrics_app_logs = (
    base
    .select(
        "*",
        F.sum("status_code_high_times_flag_bit2_avg_2_prior").over(w5).alias("running_status_code_high_times_flag_bit2_avg_2_prior"),
        F.lead("prev_status_code_high", 1).over(w6).alias("next_prev_status_code_high"),
    )
)

matched_keys = (
    metrics_app_logs
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("host") != "h2")
        .select("id")
    )
)

verified = (
    metrics_app_logs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup(
        F.when(F.col("next_status_code_high_times_flag_bit2") >= 69, "LARGE").otherwise("SMALL").alias("next_status_code_high_times_flag_bit2_tier"),
        F.when(F.col("status_code_high_avg_6") >= 3, "PREMIUM").otherwise("STANDARD").alias("status_code_high_avg_6_tier"),
        F.when(F.col("status_code_high") >= 2, "LARGE").otherwise("SMALL").alias("status_code_high_tier"),
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("status_code_high_times_flag_bit2_cume_dist") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("status_code_high_times_flag_bit2_cume_dist_per_region"),
    )
    .orderBy(F.col("version_rows").desc_nulls_last())
)
