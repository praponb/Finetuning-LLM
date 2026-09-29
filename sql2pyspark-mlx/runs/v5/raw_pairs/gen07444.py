from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("monthly_fee_tier").orderBy(F.col("consumer_monthly_fee").desc())

matched_keys = (
    df_subscriptions
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("plan") == "basic")
        .select("plan")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("end_date") > F.to_date(F.lit("2023-10-22")))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.lower("plan").alias("plan_lower"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") != "Tokyo")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.monthly_fee",
        "s.plan_lower",
        "s.monthly_fee_dense_rank",
        "s.monthly_fee_share",
        F.col("p.segment"),
        F.col("p.signup_date"),
        F.col("p.country"),
    )
)

valid_subscriptions = (
    enriched
    .filter(F.col("start_date") >= F.lit("2023-09-12").cast("date"))
    .select(
        "*",
        (F.col("monthly_fee_share") * F.col("monthly_fee_dense_rank")).alias("monthly_fee_share_times_monthly_fee_dense_rank"),
        F.least("monthly_fee", "monthly_fee_share").alias("least_monthly_fee_monthly_fee_share"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("resolved_segment"),
    )
)

with_dims = (
    valid_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.*",
        F.col("p.email"),
        F.col("p.segment").alias("customer_segment"),
        F.col("p.age"),
    )
)

totals = (
    with_dims
    .groupBy(
        F.when(F.col("monthly_fee") >= 224, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
        F.when(F.col("monthly_fee_share") >= 0.69, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_share_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("monthly_fee")).otherwise(0)).alias("us_monthly_fee"),
        F.avg(F.when(F.col("segment") == "SMB", F.col("least_monthly_fee_monthly_fee_share"))).alias("smb_avg_least_monthly_fee_monthly_fee_share"),
        F.sum(F.when(F.col("customer_segment") == "Consumer", F.col("monthly_fee")).otherwise(0)).alias("consumer_monthly_fee"),
    )
    .withColumn("consumer_monthly_fee_rank", F.rank().over(w3))
)

result = (
    totals
    .select(
        "monthly_fee_tier",
        "monthly_fee_share_tier",
        "consumer_monthly_fee",
        "consumer_monthly_fee_rank",
    )
)
