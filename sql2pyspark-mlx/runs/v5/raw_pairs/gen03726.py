from pyspark.sql import functions as F

result = (
    df_inventory
    .select(F.lit("inventory").alias("kind"), "last_restock")
    .union(df_products.select(F.lit("product").alias("kind"), "launch_date"))
)
