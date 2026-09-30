from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.hour("log_ts").between(9, 16))
    .select(
        "id",
        "log_ts",
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.countDistinct("region_or_default").alias("distinct_region_or_defaults"),
        F.min("log_ts").alias("first_seen"),
        F.min("log_ts").alias("first_log_ts"),
        F.min("log_ts").alias("first_log_ts_2"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("duration_min").desc_nulls_first())
)
