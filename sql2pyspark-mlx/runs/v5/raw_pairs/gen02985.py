from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.col("opened_date").alias("event_date"))
    .subtract(
        df_subscriptions
        .filter(F.col("plan") != "premium")
        .select(F.col("end_date").alias("event_date"))
    )
)
