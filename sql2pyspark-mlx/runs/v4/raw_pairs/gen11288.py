from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.min("start_date").alias("first_start_date"),
    )
    .filter(F.col("monthly_fee_range") >= 43)
    .select("customer_id", "monthly_fee_range", "first_start_date")
    .orderBy(F.col("customer_id").desc())
)
