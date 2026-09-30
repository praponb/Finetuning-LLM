from pyspark.sql import functions as F

result = (
    df_products
    .select("cost", F.lit("product").alias("kind"))
    .union(
        df_subscriptions
        .select("monthly_fee", F.lit("subscription").alias("kind"))
    )
    .distinct()
)
