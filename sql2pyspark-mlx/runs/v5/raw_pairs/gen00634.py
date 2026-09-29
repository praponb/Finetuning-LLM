from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("revenue").alias("max_revenue"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.cost"), F.col("ssbp.max_revenue"))
    .orderBy(F.col("cost").desc())
)
