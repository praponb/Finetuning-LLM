from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("retry_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("retry_count").desc(), "id")
w3 = Window.partitionBy("level").orderBy("log_ts", "id").rowsBetween(-3, -1)

unpacked = (
    df_app_logs
    .filter(F.col("service").isin(["checkout", "auth"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .withColumn("first_known_retry_count", F.first("retry_count", ignorenulls=True).over(w))
    .withColumn("retry_count_bucket", F.ntile(5).over(w2))
    .withColumn("retry_count_avg_3_prior", F.avg("retry_count").over(w3))
)
