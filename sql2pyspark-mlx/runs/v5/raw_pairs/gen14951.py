from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() | (F.col("end_date") >= "2023-07-01"))
    .select(
        "plan",
        "start_date",
        "end_date",
        F.add_months("end_date", 1).alias("end_date_plus_months"),
    )
    .orderBy(F.col("plan").desc(), "end_date")
)
