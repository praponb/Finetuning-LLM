from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .filter(F.col("store_sales.revenue").between(2200, 3600))
    .groupBy(F.year(F.col("stores.opened_date")).alias("opened_year"))
    .agg(
        F.min(F.col("store_sales.sale_date")).alias("min_sale_date"),
        F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"),
    )
    .orderBy(F.col("opened_year").desc())
)
