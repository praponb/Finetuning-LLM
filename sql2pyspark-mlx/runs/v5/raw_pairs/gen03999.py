from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w_part = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("running_monthly_fee")
w5 = Window.partitionBy("status").orderBy("running_max_row_seq", "id")
w6 = Window.partitionBy("status").orderBy(F.col("next_running_max_row_seq").desc_nulls_last(), "id")
w7 = Window.partitionBy("status").orderBy("next_running_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status")
w9 = Window.partitionBy("status").orderBy(F.col("row_seq").desc(), "id")
w10 = Window.orderBy(F.col("running_count_row_seq").desc(), "id")
w11 = Window.partitionBy("status").orderBy("running_min_row_seq", "id").rowsBetween(-6, Window.currentRow)
w12 = Window.partitionBy("status").orderBy(F.col("row_seq_2").desc_nulls_last())

scored = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "status",
        "running_monthly_fee",
        "row_seq",
        F.count("row_seq").over(w_part).alias("running_count_row_seq"),
        F.min("row_seq").over(w_part).alias("running_min_row_seq"),
        F.max("row_seq").over(w_part).alias("running_max_row_seq"),
        F.percent_rank().over(w4).alias("running_monthly_fee_percent_rank"),
    )
)

with_windows_subscriptions = (
    with_windows
    .withColumn("prev_running_max_row_seq", F.lag("running_max_row_seq").over(w5))
    .withColumn("next_running_max_row_seq", F.lead("running_max_row_seq").over(w5))
    .withColumn("next_running_monthly_fee", F.lead("running_monthly_fee").over(w5))
    .withColumn("row_seq_2", F.row_number().over(w5))
)

latest = (
    with_windows_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    latest
    .filter(F.col("running_monthly_fee_percent_rank").between(0.44, 0.51))
    .select(
        "id",
        "status",
        "running_monthly_fee",
        "row_seq",
        "running_count_row_seq",
        "running_min_row_seq",
        "running_max_row_seq",
        "running_monthly_fee_percent_rank",
        "prev_running_max_row_seq",
        "next_running_max_row_seq",
        "row_seq_2",
        F.last("next_running_monthly_fee", ignorenulls=True).over(w7).alias("last_known_next_running_monthly_fee"),
        F.min("row_seq").over(w8).alias("status_min_row_seq"),
        F.row_number().over(w9).alias("row_seq_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "running_count_row_seq",
        "running_monthly_fee_percent_rank",
        "row_seq_2",
        F.ntile(3).over(w10).alias("running_count_row_seq_bucket"),
        F.avg("row_seq_3").over(w11).alias("row_seq_3_avg_6"),
        F.rank().over(w12).alias("row_seq_2_rank"),
    )
)
