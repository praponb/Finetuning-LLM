from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") < 120)
    .orderBy(F.col("product_id").desc(), "store_id")
)
