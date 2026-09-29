from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.max("sale_date").alias("last_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .select(
        F.col("p.cost"),
        F.col("ssbp.unique_products"),
        F.col("ssbp.last_sale_date"),
    )
    .orderBy(F.col("cost").desc())
)
