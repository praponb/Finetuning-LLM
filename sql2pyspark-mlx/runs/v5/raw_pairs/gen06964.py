from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_last())

valid = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-08-30")))
    .withColumn("plan_prefix", F.substring("plan", 1, 3))
)

enriched = (
    valid
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("running_sum_monthly_fee"),
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.rank().over(w2).alias("monthly_fee_rank"),
    )
)

enriched_subscriptions = (
    enriched
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.end_date",
        "e.running_count_monthly_fee",
        "e.running_sum_monthly_fee",
        F.col("b.email"),
        F.col("b.country"),
        F.col("b.segment"),
    )
)

base = (
    enriched_subscriptions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "end_date",
        "running_count_monthly_fee",
        "running_sum_monthly_fee",
        "email",
        "country",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "end_date",
        "running_count_monthly_fee",
        "running_sum_monthly_fee",
        "country",
        "email_user",
        "segment_clean",
    )
)
