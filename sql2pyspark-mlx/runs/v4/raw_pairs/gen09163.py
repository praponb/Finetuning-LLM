from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([14, 20, 33, 35]) & (F.col("start_date") < "2023-09-26") & F.col("start_date").between("2024-01-01", "2025-05-01"))
    .select(
        "monthly_fee",
        "end_date",
        F.weekofyear("end_date").alias("end_date_weekofyear"),
    )
)
