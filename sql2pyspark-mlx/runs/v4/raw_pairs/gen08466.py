from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("id", "order_id")
    .unionByName(df_order_items.filter(F.col("quantity") >= 6).select("id", "order_id"))
    .orderBy("id")
)
