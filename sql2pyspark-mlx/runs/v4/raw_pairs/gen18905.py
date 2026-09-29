from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 29)
    .select(
        "start_date",
        F.when(F.col("status") == "PAUSED", 1).otherwise(0).alias("is_paused"),
    )
    .orderBy(F.col("is_paused").desc())
)
