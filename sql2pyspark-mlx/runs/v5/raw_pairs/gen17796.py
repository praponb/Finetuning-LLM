from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("num_context", "id")
w3 = Window.partitionBy("tier", "service")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context").getItem("trace").alias("trace"),
        F.size("context").alias("num_context"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "status_code",
        "has_trace",
        "tier",
        "trace",
        F.last("num_context", ignorenulls=True).over(w).alias("last_known_num_context"),
        F.lead("status_code", 1).over(w2).alias("next_status_code"),
        F.stddev_samp("status_code").over(w3).alias("tier_stddev_samp_status_code"),
    )
)
