from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.category"), F.col("p.launch_date"))
    .orderBy(F.col("id").desc(), "category")
)
