from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .select("order_id", "quantity")
    .orderBy(F.col("order_id").desc())
)
