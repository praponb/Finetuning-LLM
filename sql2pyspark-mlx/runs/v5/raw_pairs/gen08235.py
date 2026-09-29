from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_last())
w2 = Window.orderBy("monthly_fee", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("customer_id").orderBy("monthly_fee_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(~F.col("p.email").like("%@yahoo.com"))
    .select("src.*", F.col("p.country"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.signup_date"))
)

enriched_subscriptions = (
    joined
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        "country",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_6_prior"),
        F.stddev_samp("monthly_fee").over(w3).alias("plan_stddev_samp_monthly_fee"),
        F.count("monthly_fee").over(w3).alias("plan_count_monthly_fee"),
    )
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee_rank",
        "monthly_fee_avg_6_prior",
        "plan_stddev_samp_monthly_fee",
        F.avg("monthly_fee_avg_6_prior").over(w4).alias("running_avg_monthly_fee_avg_6_prior"),
        F.min("monthly_fee_avg_6_prior").over(w4).alias("running_min_monthly_fee_avg_6_prior"),
        F.sum("monthly_fee_avg_6_prior").over(w4).alias("running_sum_monthly_fee_avg_6_prior"),
        F.count("monthly_fee_avg_6_prior").over(w4).alias("running_count_monthly_fee_avg_6_prior"),
    )
)

result = (
    enriched_subscriptions_2
    .select(
        "id",
        "customer_id",
        "monthly_fee_avg_6_prior",
        "plan_stddev_samp_monthly_fee",
        "running_min_monthly_fee_avg_6_prior",
        "running_sum_monthly_fee_avg_6_prior",
        "running_count_monthly_fee_avg_6_prior",
    )
    .orderBy("monthly_fee_avg_6_prior")
)
