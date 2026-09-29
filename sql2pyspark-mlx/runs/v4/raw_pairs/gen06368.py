from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "eco") & (F.col("p.price") >= 1140))
    .select(F.col("p.supplier_id"), F.col("p.product_name"), F.col("ss.store_id"))
    .orderBy("product_name", F.col("store_id").desc())
)
