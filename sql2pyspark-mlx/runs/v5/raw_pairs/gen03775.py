from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-09-27").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.last_day("end_date").alias("end_date_month_end"),
        F.date_trunc("MONTH", "start_date").alias("start_date_period"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.status",
        "s.end_date_month_end",
        F.col("d.age"),
        F.col("d.name"),
    )
)
