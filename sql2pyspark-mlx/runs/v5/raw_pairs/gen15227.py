from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-02-28")
    .select(
        "monthly_fee",
        F.add_months("start_date", 1).alias("start_date_plus_months"),
    )
    .orderBy(F.col("start_date_plus_months").desc())
)
