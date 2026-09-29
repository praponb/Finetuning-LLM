from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-01-09", "2024-06-01") & (F.col("customer_id") == 7) & F.col("monthly_fee").between(120, 160))
    .select(
        "status",
        "customer_id",
        F.add_months("end_date", 3).alias("end_date_plus_months"),
    )
    .orderBy("customer_id")
)
