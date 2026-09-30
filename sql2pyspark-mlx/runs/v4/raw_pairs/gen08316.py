from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") < 7) & F.col("product_id").isin([6, 14, 26])) | F.col("product_id").isin([17, 23, 35]))
    .select("id", "quantity")
)
