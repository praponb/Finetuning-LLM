from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2023-10-01")
    .select(
        "id",
        "monthly_fee",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
