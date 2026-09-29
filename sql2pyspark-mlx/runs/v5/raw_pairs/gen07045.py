from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6, 7, 9]) & (F.col("quantity") >= 162))
    .withColumn("last_restock_year", F.year("last_restock"))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM-dd"))
)
