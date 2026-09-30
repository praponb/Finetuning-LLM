from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Chiang Mai", "Singapore"]))
    .select(
        "*",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
        F.coalesce("segment", F.lit("N/A")).alias("resolved_segment"),
    )
)

result = (
    base
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.monthly_fee") >= 210), "left_semi")
)
