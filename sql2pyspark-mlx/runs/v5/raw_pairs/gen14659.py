from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 335)
    .select("price")
    .subtract(df_order_items.select("unit_price"))
    .orderBy("price")
)
