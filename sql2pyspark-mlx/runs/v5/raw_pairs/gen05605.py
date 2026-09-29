from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-06-13")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

result = (
    cleaned
    .select("plan", "customer_id")
    .subtract(
        df_subscriptions
        .filter(~F.col("plan").isin(["basic", "enterprise"]))
        .select("plan", "customer_id")
    )
)
