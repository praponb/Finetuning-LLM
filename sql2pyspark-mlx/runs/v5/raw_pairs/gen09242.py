from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") > 1430)
    .orderBy("product_id")
)
