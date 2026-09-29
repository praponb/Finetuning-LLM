from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .orderBy(F.col("total_quantity").desc(), "product_id")
    .limit(20)
)
