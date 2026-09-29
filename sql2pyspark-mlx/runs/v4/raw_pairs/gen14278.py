from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("units").between(34, 35))
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .filter(F.col("ssbp.max_units") >= 30)
    .select(
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("ssbp.max_units"),
        F.col("ssbp.first_sale_date"),
    )
)
