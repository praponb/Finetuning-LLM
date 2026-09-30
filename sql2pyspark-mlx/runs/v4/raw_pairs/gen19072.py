from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("has_fault").desc_nulls_first(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

ranked = (
    flattened_attrs
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy("k")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.sum(F.when(F.col("k") == "trace", F.col("has_fault")).otherwise(0)).alias("trace_has_fault"),
    )
    .filter(F.col("avg_status_code_high") >= 0)
    .orderBy("avg_status_code_high")
)
