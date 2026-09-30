from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2024-03-03").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.substring("plan", 1, 3).alias("plan_prefix"),
        F.length("status").alias("status_length"),
    )
)

result = (
    base
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.country"))
)
