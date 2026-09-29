from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 4)
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 383, "High").when(F.col("stock_qty") >= 162, "Medium").otherwise("Low"))
)
