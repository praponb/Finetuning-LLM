from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 212) & F.col("store_id").isin([3, 6]))
    .select("id", "product_id")
)
