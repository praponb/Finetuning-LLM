from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "right")
    .filter(F.col("products.stock_qty") > 151)
    .groupBy(
        F.year(F.col("stores.opened_date")).alias("opened_year"),
        F.col("stores.city"),
    )
    .agg(
        F.countDistinct(F.col("store_sales.units")).alias("unique_products"),
        F.avg(F.col("store_sales.units")).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc(), F.col("city").desc())
)
