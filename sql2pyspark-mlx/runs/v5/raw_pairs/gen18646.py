from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "host")
w2 = Window.partitionBy("service", "host").orderBy("status_code_high", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service", "key").orderBy("last_known_latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("has_fault", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(~F.col("service").isin(["auth", "search", "billing"]))
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .filter(F.col("key") == "trace")
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("last_known_latency_ms", F.last("latency_ms", ignorenulls=True).over(w2))
)

filtered = (
    scored
    .filter(F.col("key").isNotNull() & (F.col("host") == "h2"))
    .select(
        "*",
        F.greatest("status_code_high", "has_fault").alias("greatest_status_code_high_has_fault"),
    )
)

scored_app_logs = (
    filtered
    .withColumn("running_avg_has_fault", F.avg("has_fault").over(w3))
    .withColumn("running_sum_has_fault", F.sum("has_fault").over(w3))
    .withColumn("running_min_has_fault", F.min("has_fault").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

result = (
    scored_app_logs
    .groupBy("service", "host")
    .agg(
        F.round(F.sum("running_sum_has_fault") / F.nullif(F.count(F.when(F.col("host") == "h4", 1)), F.lit(0)), 2).alias("running_sum_has_fault_per_h4"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
        F.round(F.sum("running_avg_has_fault") / F.nullif(F.count(F.when(F.col("host") == "h3", 1)), F.lit(0)), 2).alias("running_avg_has_fault_per_h3"),
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_count"),
    )
    .filter(F.col("h4_count") > 1)
)
