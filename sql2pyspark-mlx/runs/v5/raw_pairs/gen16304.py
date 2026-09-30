from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("end_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status").orderBy("start_date_month_end", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())
w5 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_cume_dist").desc_nulls_last(), "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-06-07").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.date_trunc("YEAR", "end_date").alias("end_date_period"),
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

metrics = (
    base
    .filter(F.col("end_date").between("2024-04-08", "2025-07-01"))
    .select(
        "*",
        F.count("monthly_fee").over(w).alias("monthly_fee_count_30d"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.cume_dist().over(w4).alias("monthly_fee_cume_dist"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

matched = (
    latest
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    matched
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "seq_no",
        "monthly_fee_cume_dist",
        "dedup_rank",
        "subscription_id",
        F.when(F.col("status").rlike("^(PAUSED|CANCELLED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(ACTIVE|EXPIRED)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
