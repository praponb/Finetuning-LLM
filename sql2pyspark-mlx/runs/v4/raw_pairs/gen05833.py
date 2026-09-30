from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 120)
    .select("id", "plan", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") <= "2024-03-01")
        .select("id", "plan", "end_date")
    )
    .orderBy(F.col("id").desc())
)
