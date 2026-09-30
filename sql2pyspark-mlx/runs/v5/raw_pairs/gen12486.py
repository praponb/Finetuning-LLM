from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("stock_qty") <= 241) | F.col("stock_qty").between(114, 260))
)

summary = (
    recent_products
    .groupBy("category")
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
)

result = (
    summary
    .select("category", "category_list")
    .orderBy(F.col("category").desc())
)
