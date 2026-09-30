from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("shipping_fee").alias("value"))
    .subtract(
        df_products
        .filter(F.col("stock_qty") < 191)
        .select(F.col("price").alias("value"))
    )
)
