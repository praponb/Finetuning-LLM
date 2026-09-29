from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.credit_limit"), F.col("b.segment"))
)

enriched_subscriptions = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.name"))
)

result = (
    enriched_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status",
        "credit_limit",
        "segment",
        "name",
    )
    .orderBy(F.col("credit_limit").desc_nulls_last())
)
