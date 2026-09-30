from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", F.lit("store").alias("source"))
    .union(
        df_subscriptions
        .select("end_date", F.lit("subscription").alias("source"))
    )
)
