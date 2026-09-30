from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("key").orderBy("has_fault", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("key")
w_running = Window.partitionBy("key").orderBy(F.col("has_fault").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy(F.col("status_code_high").desc_nulls_first())
w5 = Window.orderBy(F.col("status_code_high").desc(), "id", "key")
w6 = Window.partitionBy("key").orderBy(F.col("key_min_has_fault").asc_nulls_first(), "id", "key")
w7 = Window.partitionBy("key").orderBy("row_seq", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("key").orderBy("status_code_high_percent_rank", "id", "key").rowsBetween(-6, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "key",
        "v",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .filter(F.col("has_fault") <= 1)
    .select(
        "id",
        "key",
        F.sum("has_fault").over(w_part).alias("cumulative_has_fault"),
        F.min("has_fault").over(w_part_2).alias("key_min_has_fault"),
        F.row_number().over(w_running).alias("row_seq"),
        F.percent_rank().over(w4).alias("status_code_high_percent_rank"),
        F.row_number().over(w5).alias("row_seq_2"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .select("id", "key", "row_seq", "status_code_high_percent_rank")
)

enriched = (
    deduped
    .withColumn("running_sum_row_seq", F.sum("row_seq").over(w7))
    .withColumn("running_count_row_seq", F.count("row_seq").over(w7))
    .withColumn("running_max_row_seq", F.max("row_seq").over(w7))
    .withColumn("status_code_high_percent_rank_avg_6", F.avg("status_code_high_percent_rank").over(w8))
)

result = (
    enriched
    .select("id", "key", "running_sum_row_seq", "status_code_high_percent_rank")
    .unpivot(
        ids=["id", "key"],
        values=["running_sum_row_seq", "status_code_high_percent_rank"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
