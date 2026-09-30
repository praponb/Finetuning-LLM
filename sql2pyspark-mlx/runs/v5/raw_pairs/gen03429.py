from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2023-03-01") & F.col("product_id").isin([4, 6, 7, 8]))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
    .withColumn("last_restock_weekofyear", F.weekofyear("last_restock"))
)
