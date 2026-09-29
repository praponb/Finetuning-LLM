from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("units").alias("max_units"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .filter(F.col("ssbp.max_units") >= 27)
    .select(F.col("p.supplier_id"), F.col("ssbp.max_units"))
)
