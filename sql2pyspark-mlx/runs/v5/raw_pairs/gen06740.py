from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code_high").desc(), "id", "key")
w_ordered = Window.partitionBy("key").orderBy("is_flagged", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("is_flagged", "id", "key")
w4 = Window.partitionBy("key").orderBy(F.col("running_count_status_code_high").asc_nulls_first())
w5 = Window.orderBy("is_flagged_bucket", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("key") == "trace")
    .select(
        "id",
        "level",
        "key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

ranked = (
    flattened_attrs
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked
    .select(
        "id",
        "key",
        "is_flagged",
        F.sum("status_code_high").over(w_ordered).alias("running_sum_status_code_high"),
        F.avg("status_code_high").over(w_ordered).alias("running_avg_status_code_high"),
        F.count("status_code_high").over(w_ordered).alias("running_count_status_code_high"),
        F.ntile(10).over(w3).alias("is_flagged_bucket"),
    )
)

cleaned = (
    metrics
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("region"))))
    .select(
        "id",
        "key",
        "is_flagged",
        "running_avg_status_code_high",
        "running_count_status_code_high",
        "is_flagged_bucket",
        F.ceil("is_flagged_bucket").alias("is_flagged_bucket_whole"),
        F.when(F.col("is_flagged") >= 1, "High").when(F.col("is_flagged") >= 0, "Medium").otherwise("Low").alias("is_flagged_tier"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "key",
        "is_flagged",
        "is_flagged_bucket",
        F.dense_rank().over(w4).alias("running_count_status_code_high_dense_rank"),
        F.sum("is_flagged").over(w5).alias("cumulative_is_flagged"),
    )
)
