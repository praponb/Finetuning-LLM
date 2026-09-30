from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-08-23").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.add_months("start_date", 12).alias("start_date_plus_months"),
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

result = (
    cleaned
    .select("id", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").between(160, 170))
        .select("id", "plan")
    )
)
