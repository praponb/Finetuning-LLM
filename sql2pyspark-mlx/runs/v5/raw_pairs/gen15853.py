from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-09-22").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
        F.date_sub("end_date", 16).alias("end_date_minus_16d"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee_whole",
        F.max("monthly_fee_whole").over(w).alias("running_max_monthly_fee_whole"),
        F.sum("monthly_fee_whole").over(w).alias("running_sum_monthly_fee_whole"),
        F.avg("monthly_fee_whole").over(w).alias("running_avg_monthly_fee_whole"),
        F.min("monthly_fee_whole").over(w).alias("running_min_monthly_fee_whole"),
    )
)
