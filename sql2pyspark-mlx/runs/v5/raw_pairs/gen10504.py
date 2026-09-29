from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("error_events").desc())

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    unpacked
    .groupBy(
        "level",
        F.when(F.col("has_fault") >= 1, "PREMIUM").otherwise("STANDARD").alias("has_fault_tier"),
    )
    .agg(
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.sum(F.when(F.col("level") == "INFO", F.col("has_fault"))).alias("info_total_has_fault"),
        F.countDistinct("level").alias("distinct_levels"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("service") == "auth", F.col("has_fault")).otherwise(0)).alias("auth_has_fault"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .withColumn("error_events_rank", F.rank().over(w))
    .orderBy(F.col("has_fault_tier").desc_nulls_last())
)
