from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") < 390)
    .select("store_id", "total_quantity")
)
