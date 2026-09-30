from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 23]))
    .select("product_id")
    .orderBy(F.col("product_id").desc())
)
