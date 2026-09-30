from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(-6, -1)
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("service", "host").orderBy(F.col("status_code_high_avg_6_prior").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    flattened_attrs
    .select(
        "*",
        F.avg("status_code_high").over(w).alias("status_code_high_avg_6_prior"),
        F.count("flag_bit2").over(w2).alias("service_count_flag_bit2"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "host", "k", "attr_value", "flag_bit2", "service_count_flag_bit2")
)

result = (
    deduped
    .groupBy("k")
    .agg(
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.max(F.when(F.col("k") == "trace", F.col("flag_bit2"))).alias("trace_max_flag_bit2"),
        F.sum(F.when(F.col("k") == "region", F.col("service_count_flag_bit2"))).alias("region_total_service_count_flag_bit2"),
    )
    .orderBy(F.col("region_total_service_count_flag_bit2").asc_nulls_last())
)
