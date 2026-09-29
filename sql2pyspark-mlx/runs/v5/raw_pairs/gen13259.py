from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_filled").desc(), "id")
w_ordered = Window.partitionBy("customer_id").orderBy("monthly_fee_filled", "id")

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.country") == "SG")
    .select("s.*", F.col("d.signup_date"), F.col("d.credit_limit"), F.col("d.age"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "monthly_fee_filled",
        "signup_date",
        "credit_limit",
        "age",
        F.ntile(3).over(w2).alias("monthly_fee_filled_bucket"),
        F.ntile(3).over(w_ordered).alias("monthly_fee_filled_bucket_2"),
    )
)

remaining_keys = (
    scored
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") <= "2025-09-01")
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"))
    .agg(
        F.round(F.avg("monthly_fee_filled"), 2).alias("avg_monthly_fee_filled"),
        F.round(F.avg("monthly_fee_filled_bucket"), 2).alias("avg_monthly_fee_filled_bucket"),
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
        F.round(F.avg("monthly_fee_filled"), 2).alias("avg_monthly_fee_filled_2"),
        F.percentile_approx("monthly_fee_filled", 0.9).alias("monthly_fee_filled_p90"),
    )
    .filter(F.col("avg_monthly_fee_filled_bucket") < 1000000)
)
