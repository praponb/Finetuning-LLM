from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .select("product_id")
    .intersect(df_inventory.filter(F.col("quantity") > 239).select("product_id"))
)
