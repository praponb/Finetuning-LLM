from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .filter(F.col("store_sales.sale_date").between("2025-04-02", "2025-07-01"))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
        F.col("products.price"),
        F.col("products.product_name"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.date_trunc("year", F.col("store_sales.sale_date")).alias("sale_date_period"),
    )
)
