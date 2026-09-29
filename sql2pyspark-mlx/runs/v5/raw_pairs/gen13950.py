from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("product_id") == 6)
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.count("units").alias("units_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.units_count") >= 5)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("pt.last_sale_date"),
        F.col("pt.units_count"),
    )
    .orderBy(F.col("price").desc(), "product_name")
)
