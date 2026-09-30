from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("end_date", "id").rowsBetween(-3, -1)
w_recent_2 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w_grp = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("running_max_monthly_fee_avg_3_prior").asc_nulls_last(), "id")

enriched = (
    df_subscriptions
    .withColumn("monthly_fee_avg_3_prior", F.avg("monthly_fee").over(w_recent))
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w_recent_2))
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .withColumn("end_date_dayofweek", F.dayofweek("end_date"))
)

with_windows = (
    filtered
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_avg_3_prior",
        "monthly_fee_bucket",
        "end_date_dayofweek",
        F.sum("monthly_fee_avg_3_prior").over(w_grp).alias("running_sum_monthly_fee_avg_3_prior"),
        F.avg("monthly_fee_avg_3_prior").over(w_grp).alias("running_avg_monthly_fee_avg_3_prior"),
        F.min("monthly_fee_avg_3_prior").over(w_grp).alias("running_min_monthly_fee_avg_3_prior"),
        F.max("monthly_fee_avg_3_prior").over(w_grp).alias("running_max_monthly_fee_avg_3_prior"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_avg_3_prior",
        "monthly_fee_bucket",
        "end_date_dayofweek",
        "running_avg_monthly_fee_avg_3_prior",
        "running_min_monthly_fee_avg_3_prior",
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(10)
)
