from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([5, 8, 12]))
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.length("status").alias("status_length"),
        F.add_months("end_date", 1).alias("end_date_plus_months"),
    )
)
