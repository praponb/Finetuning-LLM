from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan_length_tier").orderBy(F.col("avg_monthly_fee").desc())

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-06-08")))
    .select(
        "*",
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
        F.trim("plan").alias("clean_plan"),
    )
)

kept_keys = (
    base
    .select("customer_id", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee").between(210, 220))
        .select("customer_id", "plan")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("start_date") >= F.lit("2023-09-23").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.length("plan").alias("plan_length"))
)

joined = (
    filtered
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.signup_date"), F.col("p.name"), F.col("p.segment"))
)

cleaned = (
    joined
    .filter((F.col("end_date") >= F.lit("2023-09-18").cast("date")) & F.col("plan_length").between(36, 38))
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("plan_length") >= 90, "PREMIUM").otherwise("STANDARD").alias("plan_length_tier"),
        F.when(F.col("monthly_fee") >= 183, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("plan_length", [0.25, 0.5, 0.75]).alias("plan_length_quartiles"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.percentile_approx("plan_length", 0.95).alias("plan_length_p95"),
    )
    .withColumn("avg_monthly_fee_percent_rank", F.percent_rank().over(w))
)
