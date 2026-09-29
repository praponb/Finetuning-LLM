from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy("status", "city").orderBy("credit_limit", "id")
w6 = Window.partitionBy("customer_city", "city").orderBy("credit_limit_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_city", "city").orderBy("monthly_fee", "id")
w8 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("customer_city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("customer_id")
w11 = Window.orderBy("running_monthly_fee", "id")

metrics = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_2"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.monthly_fee",
        "src.status",
        F.col("b.city"),
        F.col("b.credit_limit"),
        F.col("b.email"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

metrics_subscriptions = (
    deduped
    .select(
        "*",
        F.ntile(3).over(w4).alias("credit_limit_bucket"),
        F.lead("credit_limit", 1).over(w5).alias("next_credit_limit"),
    )
)

with_dims = (
    metrics_subscriptions
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.city").alias("customer_city"))
)

with_windows = (
    with_dims
    .filter(F.col("city").isin(["Bangkok", "Chiang Mai", "Tokyo"]))
    .select(
        "id",
        "customer_id",
        "status",
        "city",
        "latest_rank",
        "credit_limit_bucket",
        F.sum("monthly_fee").over(w6).alias("running_monthly_fee"),
        F.lead("credit_limit", 2).over(w7).alias("next_credit_limit_2"),
        F.avg("credit_limit").over(w8).alias("credit_limit_avg_6_prior"),
        F.sum("next_credit_limit").over(w9).alias("running_next_credit_limit"),
    )
)

enriched_subscriptions = (
    with_windows
    .select(
        "id",
        "status",
        "latest_rank",
        "credit_limit_bucket",
        "running_monthly_fee",
        "next_credit_limit_2",
        "credit_limit_avg_6_prior",
        F.round((F.col("credit_limit_avg_6_prior") / F.sum("credit_limit_avg_6_prior").over(w10)), 4).alias("credit_limit_avg_6_prior_share"),
        F.lag("credit_limit_avg_6_prior", 2, 0).over(w11).alias("prev_credit_limit_avg_6_prior"),
        F.lead("running_monthly_fee", 1).over(w11).alias("next_running_monthly_fee"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "id",
        "latest_rank",
        "credit_limit_bucket",
        "running_monthly_fee",
        "next_credit_limit_2",
        "prev_credit_limit_avg_6_prior",
        "next_running_monthly_fee",
    )
)
