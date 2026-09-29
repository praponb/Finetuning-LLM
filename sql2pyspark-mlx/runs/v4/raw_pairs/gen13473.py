from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 8)
    .select("order_id")
    .unionByName(df_order_items.filter(F.col("product_id") == 10).select("order_id"))
    .distinct()
)
