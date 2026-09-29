from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("end_date").alias("event_date"), F.lit("subscription").alias("kind"))
    .unionByName(
        df_products
        .filter(F.col("brand").isin(["Acme", "Umbrella", "Initech"]))
        .select(F.col("launch_date").alias("event_date"), F.lit("product").alias("kind"))
    )
    .distinct()
    .orderBy("kind")
)
