from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("tier").orderBy(F.col("status_code").desc(), "id")

unpacked = (
    df_app_logs
    .filter(F.col("service").isin(["auth", "search"]))
    .select(
        "id",
        "service",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    unpacked
    .withColumn("running_count_status_code", F.count("status_code").over(w_ordered))
    .withColumn("running_avg_status_code", F.avg("status_code").over(w_ordered))
    .withColumn("running_sum_status_code", F.sum("status_code").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w2))
)
