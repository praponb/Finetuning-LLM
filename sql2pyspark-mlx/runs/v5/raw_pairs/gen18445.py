from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "host",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("host") != "h1")
    .select(
        "id",
        "host",
        "value",
        "flag_bit8",
        (F.col("status_code_high") * F.col("flag_bit8")).alias("status_code_high_times_flag_bit8"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("status_code_high_times_flag_bit8") < 58)
    .withColumn("clean_host", F.trim("host"))
    .withColumn("host_lower", F.lower("host"))
)

base = (
    filtered_app_logs
    .filter(F.col("status_code_high_times_flag_bit8") > 72)
    .select(
        "host",
        "flag_bit8",
        "status_code_high_times_flag_bit8",
        "clean_host",
        "host_lower",
        F.least("flag_bit8", "status_code_high_times_flag_bit8").alias("least_flag_bit8_status_code_high_times_flag_bit8"),
    )
)

base_app_logs = (
    base
    .filter(F.col("least_flag_bit8_status_code_high_times_flag_bit8") > 31)
    .select(
        "*",
        F.coalesce("status_code_high_times_flag_bit8", F.lit(0)).alias("status_code_high_times_flag_bit8_filled"),
    )
)

metrics = (
    base_app_logs
    .select(
        "*",
        F.stddev_samp("least_flag_bit8_status_code_high_times_flag_bit8").over(w).alias("host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8") >= 73, "PREMIUM").otherwise("STANDARD").alias("host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8_tier"),
        "host",
    )
    .agg(
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.percentile_approx("least_flag_bit8_status_code_high_times_flag_bit8", 0.9).alias("least_flag_bit8_status_code_high_times_flag_bit8_p90"),
    )
    .orderBy(
        F.col("host_stddev_samp_least_flag_bit8_status_code_high_times_flag_bit8_tier").desc(),
    )
)
