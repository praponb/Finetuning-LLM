from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.max_units") >= 18)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("pt.latest_sale_date"),
        F.col("pt.max_units"),
    )
    .orderBy("latest_sale_date")
)
