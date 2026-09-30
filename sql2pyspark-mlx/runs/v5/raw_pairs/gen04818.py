from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code_high").asc_nulls_first(), "id", "k")

flattened = (
    df_app_logs
    .select("id", "host", "level", "status_code", F.explode("context").alias("k", "v"))
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "level",
        "k",
        "v",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    flattened_attrs
    .filter(F.col("status_code_high") == 1)
    .select("*", F.concat_ws("/", "k", "level").alias("k_level_key"))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy("level")
    .pivot("k", ["trace", "version"])
    .agg(F.sum("status_code_high"))
)
