from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 10]) & F.col("store_id").isin([1, 3]))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
    .withColumn("quantity_num", F.col("quantity").cast("string"))
    .orderBy(F.col("quantity").desc(), F.col("last_restock_label").desc())
)
