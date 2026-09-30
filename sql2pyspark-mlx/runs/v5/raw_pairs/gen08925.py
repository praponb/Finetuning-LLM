from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first(), "id")
w2 = Window.partitionBy("plan").orderBy("latest_rank", "id")
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("plan").orderBy("monthly_fee")
w5 = Window.partitionBy("plan").orderBy("monthly_fee_bucket", "id")
w6 = Window.orderBy(F.col("prev_monthly_fee").desc_nulls_last())
w7 = Window.orderBy(F.col("monthly_fee_bucket").desc(), "id")
w8 = Window.orderBy(F.col("prev_monthly_fee").desc(), "id")
w9 = Window.orderBy("monthly_fee_bucket", "id")
w10 = Window.orderBy("monthly_fee_dense_rank", "id").rowsBetween(-3, -1)

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-06-27").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
        F.ntile(10).over(w3).alias("monthly_fee_bucket"),
    )
)

metrics_subscriptions = (
    metrics
    .select(
        "id",
        "prev_monthly_fee",
        "monthly_fee_bucket",
        F.dense_rank().over(w4).alias("monthly_fee_dense_rank"),
        F.lead("prev_monthly_fee", 2).over(w5).alias("next_prev_monthly_fee"),
        F.percent_rank().over(w6).alias("prev_monthly_fee_percent_rank"),
    )
)

with_windows = (
    metrics_subscriptions
    .withColumn("monthly_fee_bucket_bucket", F.ntile(4).over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("row_seq", F.row_number().over(w9))
    .withColumn("monthly_fee_dense_rank_avg_3_prior", F.avg("monthly_fee_dense_rank").over(w10))
)

result = (
    with_windows
    .select(
        "id",
        "prev_monthly_fee",
        "monthly_fee_bucket",
        "monthly_fee_dense_rank",
        "prev_monthly_fee_percent_rank",
        "monthly_fee_bucket_bucket",
        "seq_no",
    )
    .orderBy(
        F.col("prev_monthly_fee").desc_nulls_last(),
        "prev_monthly_fee_percent_rank",
    )
)
