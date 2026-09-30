from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .unionByName(
        df_products
        .filter(F.col("stock_qty") != 230)
        .select(F.col("price").alias("shipping_fee"))
    )
    .orderBy("shipping_fee")
)
