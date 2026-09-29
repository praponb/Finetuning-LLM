from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_products
        .select(F.col("launch_date").alias("start_date"), F.lit("product").alias("source"))
    )
    .distinct()
)
