from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["enterprise", "basic", "premium"]) & F.col("end_date").between("2023-05-18", "2025-05-01"))
    .select("id", "start_date", "monthly_fee")
)
