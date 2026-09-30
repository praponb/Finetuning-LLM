from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan")
w2 = Window.partitionBy("status")
w3 = Window.orderBy(F.col("is_expired").desc(), "id")
w4 = Window.orderBy("monthly_fee_filled", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("monthly_fee_share", "id")
w6 = Window.partitionBy("status").orderBy("is_expired", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status", "customer_id")
w8 = Window.partitionBy("customer_id").orderBy("prev_customer_id_stddev_samp_monthly_fee", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("customer_id", "plan").orderBy("customer_id_stddev_samp_monthly_fee", "id")
w10 = Window.orderBy("monthly_fee_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_filled").asc_nulls_first())
w12 = Window.partitionBy("plan").orderBy("first_known_is_expired_bucket", "id").rowsBetween(-6, -1)
w13 = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-09-04").cast("date")) & F.col("customer_id").isNotNull() & (F.col("monthly_fee") > 130))
    .select(
        "*",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
        F.when(F.col("status") == "EXPIRED", 1).otherwise(0).alias("is_expired"),
    )
)

with_windows = (
    filtered
    .withColumn("customer_id_stddev_samp_monthly_fee", F.stddev_samp("monthly_fee").over(w))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
    .withColumn("is_expired_bucket", F.ntile(10).over(w3))
    .withColumn("monthly_fee_avg_3", F.avg("monthly_fee").over(w4))
)

with_windows_subscriptions = (
    with_windows
    .select(
        "*",
        F.lag("customer_id_stddev_samp_monthly_fee", 2).over(w5).alias("prev_customer_id_stddev_samp_monthly_fee"),
        F.first("is_expired_bucket", ignorenulls=True).over(w6).alias("first_known_is_expired_bucket"),
        F.round((F.col("is_expired_bucket") / F.sum("is_expired_bucket").over(w7)), 4).alias("is_expired_bucket_share"),
    )
)

scored = (
    with_windows_subscriptions
    .select(
        "*",
        F.avg("first_known_is_expired_bucket").over(w8).alias("first_known_is_expired_bucket_avg_2_prior"),
        F.ntile(10).over(w9).alias("customer_id_stddev_samp_monthly_fee_bucket"),
        F.last("monthly_fee_share", ignorenulls=True).over(w10).alias("last_known_monthly_fee_share"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.rank().over(w11))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.round((F.col("customer_id_stddev_samp_monthly_fee") / F.sum("customer_id_stddev_samp_monthly_fee").over(w2)), 4).alias("customer_id_stddev_samp_monthly_fee_share"),
        F.avg("monthly_fee_avg_3").over(w12).alias("monthly_fee_avg_3_avg_6_prior"),
        F.sum("monthly_fee_filled").over(w13).alias("cumulative_monthly_fee_filled"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.signup_date"), F.col("c.segment"))
)

result = (
    enriched
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "monthly_fee_filled",
        "is_expired",
        "customer_id_stddev_samp_monthly_fee",
        "monthly_fee_share",
        F.when(F.col("plan").rlike("^(premium|enterprise)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(basic|standard)$"), "EXTENDED").otherwise("GROUP_B").alias("plan_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy("end_date")
)
