from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("retry_count").desc_nulls_first())
w2 = Window.partitionBy("service").orderBy(F.col("status_code").desc(), "id")

unpacked = (
    df_app_logs
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "service",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .withColumn("retry_count_rank", F.rank().over(w))
    .withColumn("status_code_bucket", F.ntile(10).over(w2))
)
