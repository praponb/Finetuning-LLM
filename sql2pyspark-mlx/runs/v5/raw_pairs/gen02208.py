from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.supplier_id").isNull())
    .select("a.id", "a.product_id", "a.last_restock", F.col("d.launch_date"))
)
