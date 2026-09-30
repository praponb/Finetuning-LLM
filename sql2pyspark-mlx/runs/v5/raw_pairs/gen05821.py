from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-07-13").cast("date")) & (F.col("plan") == "basic"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.start_date",
        F.col("b.country"),
        F.col("b.signup_date"),
        F.col("b.segment"),
    )
)
