from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .union(df_orders.select("shipping_fee"))
    .distinct()
    .orderBy(F.col("revenue").desc())
)
