from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.price") > 760)
    .select(F.col("p.id"), F.col("p.supplier_id"))
    .orderBy(F.col("supplier_id").desc(), "id")
    .limit(10)
)
