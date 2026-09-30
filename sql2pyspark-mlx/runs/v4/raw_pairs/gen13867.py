from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 210) & F.col("start_date").between("2024-07-12", "2024-10-01"))
    .select(
        "monthly_fee",
        "customer_id",
        F.date_format("start_date", "MMM yyyy").alias("start_date_label"),
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
    .orderBy("days_since_end_date", F.col("monthly_fee").desc())
)
