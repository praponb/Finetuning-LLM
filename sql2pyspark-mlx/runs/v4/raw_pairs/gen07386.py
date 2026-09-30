from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("key") == "region", 1)).alias("region_rows"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.min("log_ts").alias("started_at"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("region_rows") > 1)
)
