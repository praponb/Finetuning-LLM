from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("origin"), "price")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("origin"), "monthly_fee")
    )
    .orderBy("price")
)
