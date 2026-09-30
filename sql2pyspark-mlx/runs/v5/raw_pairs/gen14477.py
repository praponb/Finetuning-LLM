from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.col("opened_date").alias("event_date"))
    .subtract(
        df_subscriptions
        .filter(F.col("plan").isin(["premium", "basic"]))
        .select(F.col("start_date").alias("event_date"))
    )
    .orderBy(F.col("event_date").desc())
)
