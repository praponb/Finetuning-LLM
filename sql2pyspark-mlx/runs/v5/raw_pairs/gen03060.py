from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 81) & F.col("quantity").between(201, 207))
    .select("store_id")
)
