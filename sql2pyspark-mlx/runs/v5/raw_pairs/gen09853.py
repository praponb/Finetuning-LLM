from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull() & (F.col("unit_price") > 130))
    .select("product_id", "order_id", "quantity")
    .orderBy(F.col("quantity").desc(), F.col("order_id").desc())
)
