from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.units"),
        F.col("products.cost"),
        F.col("products.category"),
        F.col("stores.region"),
        F.col("stores.store_name"),
    )
    .orderBy(F.col("cost").desc(), "id")
    .limit(5)
)
