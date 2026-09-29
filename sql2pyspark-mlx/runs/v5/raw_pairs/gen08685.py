from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6, 10]) & (F.col("quantity") >= 116) & (F.col("product_id") == 10))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 209, "High").when(F.col("quantity") >= 80, "Medium").otherwise("Low"))
    .withColumn("last_restock_period", F.date_trunc("YEAR", "last_restock"))
)
