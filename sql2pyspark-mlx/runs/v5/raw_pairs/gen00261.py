from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy(F.col("days_end_date_to_start_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w_ordered = Window.partitionBy("status").orderBy("days_end_date_to_start_date", "id")
w4 = Window.partitionBy("status").orderBy(F.col("seq_no").desc(), "id")
w5 = Window.orderBy("next_days_end_date_to_start_date")
w6 = Window.partitionBy("status").orderBy("start_date", "id")
w_running = Window.partitionBy("status").orderBy("next_next_days_end_date_to_start_date", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("status") != "PAUSED"))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)

scored = (
    filtered
    .filter(F.col("status").isin(["ACTIVE", "PAUSED", "CANCELLED"]))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w2))
    .withColumn("days_end_date_to_start_date_bucket", F.ntile(10).over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("next_days_end_date_to_start_date", F.lead("days_end_date_to_start_date", 1, 0).over(w_ordered))
)

valid = (
    scored
    .filter((F.col("end_date") > F.to_date(F.lit("2023-06-17"))) & (F.col("seq_no") != 6))
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "seq_no",
        "monthly_fee_dense_rank",
        "days_end_date_to_start_date_bucket",
        "next_days_end_date_to_start_date",
        F.when(F.col("status").isin(["CANCELLED", "ACTIVE"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)

with_windows = (
    valid
    .withColumn("seq_no_bucket", F.ntile(5).over(w4))
    .withColumn("next_days_end_date_to_start_date_dense_rank", F.dense_rank().over(w5))
    .withColumn("next_next_days_end_date_to_start_date", F.lead("next_days_end_date_to_start_date", 1).over(w6))
)

metrics = (
    with_windows
    .select(
        "id",
        "status",
        "seq_no",
        "monthly_fee_dense_rank",
        "days_end_date_to_start_date_bucket",
        "seq_no_bucket",
        "next_days_end_date_to_start_date_dense_rank",
        F.min("next_days_end_date_to_start_date_dense_rank").over(w_running).alias("last7_min_next_days_end_date_to_start_date_dense_rank"),
        F.avg("next_days_end_date_to_start_date_dense_rank").over(w_running).alias("last7_avg_next_days_end_date_to_start_date_dense_rank"),
        F.max("next_days_end_date_to_start_date_dense_rank").over(w_running).alias("last7_max_next_days_end_date_to_start_date_dense_rank"),
        F.sum("next_days_end_date_to_start_date_dense_rank").over(w_running).alias("last7_sum_next_days_end_date_to_start_date_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "status",
        F.col("last7_max_next_days_end_date_to_start_date_dense_rank").alias("LAST7_MAX_NEXT_DAYS_END_DATE_TO_START_DATE_DENSE_RANK"),
        F.col("last7_min_next_days_end_date_to_start_date_dense_rank").alias("LAST7_MIN_NEXT_DAYS_END_DATE_TO_START_DATE_DENSE_RANK"),
        "last7_sum_next_days_end_date_to_start_date_dense_rank",
    )
    .unpivot(
        ids=["id", "status"],
        values=["LAST7_MAX_NEXT_DAYS_END_DATE_TO_START_DATE_DENSE_RANK", "LAST7_MIN_NEXT_DAYS_END_DATE_TO_START_DATE_DENSE_RANK", "last7_sum_next_days_end_date_to_start_date_dense_rank"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
