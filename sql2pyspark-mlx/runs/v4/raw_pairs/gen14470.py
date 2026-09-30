from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("level").orderBy(F.col("status_code_high").desc(), "id")
w_grp = Window.orderBy("status_code", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context").getItem("version").alias("version"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    flattened_attrs
    .withColumn("status_code_high_bucket", F.ntile(3).over(w_ordered))
    .withColumn("status_code_bucket", F.ntile(5).over(w_grp))
)
