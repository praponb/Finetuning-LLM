from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("end_date_month_end", "id")
w3 = Window.partitionBy("status").orderBy(F.col("end_date_month_end").desc_nulls_last(), "id")
w4 = Window.orderBy("running_max_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("running_avg_monthly_fee_rounded").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(~F.col("p.is_active"))
    .select("s.*", F.col("p.age"), F.col("p.name"))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "end_date",
        "end_date_month_end",
        "running_max_monthly_fee",
        "running_count_monthly_fee",
        "running_avg_monthly_fee",
    )
)

cleaned = (
    latest
    .filter(F.col("end_date") <= "2023-09-12")
    .select(
        "id",
        "end_date",
        "end_date_month_end",
        "running_max_monthly_fee",
        "running_count_monthly_fee",
        "running_avg_monthly_fee",
        F.round("running_avg_monthly_fee", 0).alias("running_avg_monthly_fee_rounded"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "end_date_month_end",
        "running_max_monthly_fee",
        "running_count_monthly_fee",
        "running_avg_monthly_fee",
        "running_avg_monthly_fee_rounded",
        F.sum("running_count_monthly_fee").over(w4).alias("running_running_count_monthly_fee"),
        F.ntile(4).over(w5).alias("running_avg_monthly_fee_rounded_bucket"),
    )
)
