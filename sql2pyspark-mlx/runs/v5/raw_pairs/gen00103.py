from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 14822)
    .select(F.lit("store").alias("origin"), "opened_date")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("origin"), "start_date")
    )
    .distinct()
    .orderBy("origin")
)
