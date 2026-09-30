from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("end_date_month_end").desc(), "id")
w3 = Window.partitionBy("status", "plan").orderBy("end_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan", "status").orderBy(F.col("cumulative_monthly_fee").desc(), "id")
w6 = Window.partitionBy("plan", "status").orderBy("first_known_monthly_fee_whole", "id")
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("status", "plan").orderBy("row_num", "id")
w9 = Window.partitionBy("plan")
w10 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2024-04-21").cast("date")) & F.col("customer_id").isNotNull() & F.month("end_date").isin([5, 6, 11]))
    .withColumn("end_date_month_end", F.last_day("end_date"))
)

valid = (
    base
    .filter((F.col("end_date") > F.to_date(F.lit("2023-05-28"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "end_date_month_end",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_5_prior"),
        F.first("monthly_fee_whole", ignorenulls=True).over(w4).alias("first_known_monthly_fee_whole"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "*",
        F.ntile(3).over(w6).alias("first_known_monthly_fee_whole_bucket"),
        F.stddev_samp("first_known_monthly_fee_whole").over(w7).alias("status_stddev_samp_first_known_monthly_fee_whole"),
        F.lag("monthly_fee_whole", 2).over(w8).alias("prev_monthly_fee_whole"),
        F.avg("row_seq").over(w9).alias("plan_avg_row_seq"),
        F.last("first_known_monthly_fee_whole", ignorenulls=True).over(w10).alias("last_known_first_known_monthly_fee_whole"),
    )
)
