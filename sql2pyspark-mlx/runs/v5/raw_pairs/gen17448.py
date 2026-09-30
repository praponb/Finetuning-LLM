from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") == 2025)
    .select(F.lit("store").alias("origin"), "opened_date")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("origin"), "end_date")
    )
    .orderBy(F.col("origin").desc())
)
