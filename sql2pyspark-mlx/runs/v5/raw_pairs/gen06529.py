from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("cost") < 370))
    .withColumn("product_name_length", F.length("product_name"))
)

result = (
    valid
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") > "2024-11-01"), "left_semi")
)
