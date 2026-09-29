from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "tier").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("tier", "host").orderBy("latency_ms", "id")
w3 = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("tier", "host").orderBy("latency_ms", "id").rowsBetween(-3, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(~F.col("host").isin(["h4", "h3", "h1"]))
    .select(
        "id",
        "host",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.col("context").getItem("region").alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("latency_ms") < 2047)
    .withColumn("first_known_tier", F.first("tier", ignorenulls=True).over(w))
    .withColumn("prev_latency_ms", F.lag("latency_ms", 2).over(w2))
    .withColumn("last_known_tier", F.last("tier", ignorenulls=True).over(w3))
    .withColumn("latency_ms_avg_3", F.avg("latency_ms").over(w4))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w3))
)
