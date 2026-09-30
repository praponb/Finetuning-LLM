from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("quantity").between(151, 224) & F.month("last_restock").isin([3, 6, 8])) | (F.col("product_id") == 8)))
)
