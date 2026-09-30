from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "key")
w2 = Window.partitionBy("key").orderBy("status_code", "id", "key")
w3 = Window.partitionBy("key").orderBy(F.col("rn").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy("rn", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key").orderBy("is_flagged", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("rn", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

latest = (
    flattened_attrs
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "status_code", "key", "attr_value", "status_code_high", "is_flagged")
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.sum("status_code").over(w4).alias("cumulative_status_code"),
        F.sum("status_code_high").over(w4).alias("cumulative_status_code_high"),
        F.sum("status_code_high").over(w5).alias("running_status_code_high"),
        F.sum("is_flagged").over(w6).alias("running_is_flagged"),
    )
)

result = (
    enriched
    .groupBy("key")
    .agg(
        F.round(F.sum("cumulative_status_code_high") / F.nullif(F.count(F.when(F.col("key") == "version", 1)), F.lit(0)), 2).alias("cumulative_status_code_high_per_version"),
        F.count_if(F.col("key") == "trace").alias("trace_events"),
    )
    .filter(F.col("cumulative_status_code_high_per_version") < 1000000)
)
