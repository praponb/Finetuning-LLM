from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter((F.col("store_sales.sale_date") >= "2024-09-01") & (F.col("store_sales.sale_date") < "2024-10-01"))
    .select(
        F.col("inventory.last_restock"),
        F.col("inventory.quantity"),
        F.col("store_sales.store_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.units"),
        F.datediff(F.current_date(), F.col("store_sales.sale_date")).alias("days_since_sale_date"),
    )
    .orderBy(F.col("quantity").desc())
)
