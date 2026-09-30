from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.lit("store").alias("record_type"), "opened_date")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("record_type"), "start_date")
    )
    .distinct()
)
