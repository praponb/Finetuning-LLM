from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") < 240)
    .select("store_id", "max_quantity")
)
