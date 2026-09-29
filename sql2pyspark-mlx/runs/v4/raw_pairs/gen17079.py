from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("plan")
w3 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-06-20")
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.date_sub("end_date", 46).alias("end_date_minus_46d"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.stddev_samp("monthly_fee").over(w2).alias("plan_stddev_samp_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w3).alias("last_known_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w4).alias("first_known_monthly_fee"),
        F.percent_rank().over(w5).alias("monthly_fee_percent_rank"),
    )
)
