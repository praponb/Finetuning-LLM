from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 142)
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") >= 190)
)
