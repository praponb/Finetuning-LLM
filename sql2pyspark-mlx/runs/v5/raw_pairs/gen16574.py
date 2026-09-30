from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("kind"), "shipping_fee")
    .union(df_store_sales.select(F.lit("store_sale").alias("kind"), "revenue"))
    .orderBy(F.col("shipping_fee").desc())
)
