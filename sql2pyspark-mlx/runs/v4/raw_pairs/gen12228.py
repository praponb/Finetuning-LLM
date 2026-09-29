from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc(), "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("service").isin(["billing", "checkout", "search"]))
    .select(
        "id",
        "service",
        "log_ts",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.sum("status_code_high").over(w).alias("status_code_high_sum_2h"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
