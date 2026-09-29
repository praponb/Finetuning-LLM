from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.country"), F.col("d.segment"), F.col("d.credit_limit"))
)

with_windows = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "country",
        "segment",
        F.row_number().over(w).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
    )
)

valid = (
    with_windows
    .filter(F.col("segment").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.col("event_seq") * F.col("monthly_fee")).alias("event_seq_times_monthly_fee"),
        F.least("monthly_fee", "event_seq").alias("least_monthly_fee_event_seq"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped
    .filter(F.col("country") == "US")
    .withColumn("event_seq_str", F.col("event_seq").cast("double"))
    .withColumn("country_filled", F.coalesce("country", F.lit("N/A")))
)

result = (
    filtered
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "segment",
        "event_seq",
        "first_known_country",
        "event_seq_times_monthly_fee",
        "least_monthly_fee_event_seq",
        "country_clean",
        "rank_in_group",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy("segment")
)
