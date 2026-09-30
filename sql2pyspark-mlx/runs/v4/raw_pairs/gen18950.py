from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select("launch_date", F.lit("product").alias("source"))
    .union(
        df_subscriptions
        .select("end_date", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
