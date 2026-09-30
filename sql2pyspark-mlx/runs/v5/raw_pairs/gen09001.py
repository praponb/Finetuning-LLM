from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("shipping_fee").alias("value"), F.lit("order").alias("kind"))
    .unionByName(
        df_products
        .filter(F.col("stock_qty") < 280)
        .select(F.col("price").alias("value"), F.lit("product").alias("kind"))
    )
    .distinct()
)
