from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    unpacked
    .withColumn("is_flagged_tier", F.when(F.col("is_flagged") >= 1, "HIGH").otherwise("LOW"))
    .rollup("is_flagged_tier")
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.size(F.collect_set("service")).alias("num_services_2"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("billing_count").asc_nulls_first())
)
