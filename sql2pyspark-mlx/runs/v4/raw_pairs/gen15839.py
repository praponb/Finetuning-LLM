from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", F.lit("store").alias("source"))
    .union(
        df_subscriptions
        .filter(F.col("status") == "ACTIVE")
        .select("end_date", F.lit("subscription").alias("source"))
    )
)
