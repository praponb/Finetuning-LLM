from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-11-01", "2025-02-19"))
    .select("plan")
    .unionByName(df_subscriptions.filter(F.col("monthly_fee") >= 240).select("plan"))
    .distinct()
)
