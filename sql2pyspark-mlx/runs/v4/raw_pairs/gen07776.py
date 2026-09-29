from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("product_id").isin([6, 8, 9]))
    .select("*", F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"))
)

result = (
    valid
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.launch_date"))
)
