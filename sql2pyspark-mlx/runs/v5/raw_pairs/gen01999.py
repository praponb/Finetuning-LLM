from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5, 6]) & F.col("product_id").isin([1, 7]))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 180, "High").when(F.col("quantity") >= 84, "Medium").otherwise("Low"))
    .orderBy("quantity_num")
)
