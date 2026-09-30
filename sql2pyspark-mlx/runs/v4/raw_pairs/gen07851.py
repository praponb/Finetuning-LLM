from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_store_sales
        .filter(F.col("product_id").isin([1, 2]))
        .select("sale_date", F.lit("store_sale").alias("source"))
    )
)
