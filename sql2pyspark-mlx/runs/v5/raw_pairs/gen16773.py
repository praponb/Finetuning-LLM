from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy("quantity_count")
)
