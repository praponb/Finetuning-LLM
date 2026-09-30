from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .union(df_orders.filter(F.col("shipping_fee") >= 19).select("order_date"))
    .orderBy(F.col("sale_date").desc())
)
