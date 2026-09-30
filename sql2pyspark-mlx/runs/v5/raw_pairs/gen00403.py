from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 9, 12]))
    .withColumn("product_name_brand_key", F.concat_ws(",", "product_name", "brand"))
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 355, "High").when(F.col("stock_qty") >= 191, "Medium").otherwise("Low"))
)
