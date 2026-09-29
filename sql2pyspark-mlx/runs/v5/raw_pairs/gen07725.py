from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .select("shipping_fee")
    .union(df_products.select("cost"))
    .distinct()
)
