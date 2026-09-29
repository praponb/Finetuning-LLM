from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee_whole").desc_nulls_first())
w_recent = Window.partitionBy("status").orderBy("running_count_monthly_fee_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("running_count_monthly_fee_whole").asc_nulls_last())

cleaned = (
    df_subscriptions
    .filter(F.year("start_date") >= 2025)
    .select(
        "id",
        "start_date",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("monthly_fee_whole").isNotNull())
    .select(
        "id",
        "start_date",
        "status",
        F.count("monthly_fee_whole").over(w_grp).alias("running_count_monthly_fee_whole"),
        F.min("monthly_fee_whole").over(w_grp).alias("running_min_monthly_fee_whole"),
        F.sum("monthly_fee_whole").over(w_grp).alias("running_sum_monthly_fee_whole"),
        F.dense_rank().over(w2).alias("monthly_fee_whole_dense_rank"),
    )
)

base = (
    with_windows
    .filter((F.col("start_date") > F.lit("2023-03-25").cast("date")) & F.col("start_date").between("2023-08-20", "2025-10-01"))
    .select(
        "id",
        "start_date",
        "status",
        "running_count_monthly_fee_whole",
        F.col("monthly_fee_whole_dense_rank").cast("double").alias("monthly_fee_whole_dense_rank_num"),
    )
)

scored = (
    base
    .select(
        "id",
        "status",
        "running_count_monthly_fee_whole",
        "monthly_fee_whole_dense_rank_num",
        F.min("running_count_monthly_fee_whole").over(w_recent).alias("running_min_running_count_monthly_fee_whole"),
        F.sum("running_count_monthly_fee_whole").over(w_recent).alias("running_sum_running_count_monthly_fee_whole"),
        F.avg("running_count_monthly_fee_whole").over(w_recent).alias("running_avg_running_count_monthly_fee_whole"),
        F.dense_rank().over(w4).alias("running_count_monthly_fee_whole_dense_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "status",
        "running_count_monthly_fee_whole",
        "monthly_fee_whole_dense_rank_num",
        "running_min_running_count_monthly_fee_whole",
        "running_sum_running_count_monthly_fee_whole",
        "running_count_monthly_fee_whole_dense_rank",
        F.when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "EXTENDED").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("running_count_monthly_fee_whole_dense_rank").asc_nulls_last())
)
