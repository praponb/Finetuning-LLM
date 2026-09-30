from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("product_name").like("D%"))
    .select(F.lit("product").alias("kind"), "cost")
    .union(
        df_subscriptions
        .select(F.lit("subscription").alias("kind"), "monthly_fee")
    )
    .distinct()
    .orderBy("cost")
)
