from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id")

filtered = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-03-30").cast("date"))
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
    .withColumn("monthly_fee_whole", F.ceil("monthly_fee"))
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "plan", "start_date", "end_date")
)

ranked_subscriptions = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "start_date")
)

base = (
    ranked_subscriptions
    .filter(F.col("start_date").between("2024-04-01", "2024-11-01"))
    .withColumn("start_date_label", F.date_format("start_date", "yyyy-MM-dd"))
)

cleaned = (
    base
    .filter(F.col("start_date") >= F.lit("2023-03-08").cast("date"))
    .withColumn("start_date_quarter", F.quarter("start_date"))
)

base_subscriptions = (
    cleaned
    .filter(F.col("start_date").between("2023-07-18", "2023-11-01"))
    .select(
        "*",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

result = (
    base_subscriptions
    .cube(F.date_trunc("WEEK", "start_date").alias("week_start"))
    .agg(
        F.round(F.avg("days_since_start_date"), 2).alias("avg_days_since_start_date"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("start_date_quarter", [0.25, 0.5, 0.75]).alias("start_date_quarter_quartiles"),
        F.percentile_approx("days_since_start_date", 0.9).alias("days_since_start_date_p90"),
        F.round(F.avg("days_since_start_date"), 2).alias("avg_days_since_start_date_2"),
    )
    .filter(F.col("avg_days_since_start_date_2") >= 0)
    .orderBy(F.col("week_start").desc())
)
