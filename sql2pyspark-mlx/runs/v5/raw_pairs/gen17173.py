from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("units").isNotNull()).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.col("p.category") == "Grocery")
    .select(F.col("p.id"), F.col("p.launch_date"))
)
