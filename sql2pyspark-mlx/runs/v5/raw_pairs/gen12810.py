from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .groupBy(
        F.date_trunc("MONTH", F.col("store_sales.sale_date")).alias("sale_month"),
        F.col("store_sales.product_id"),
    )
    .agg(
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
    )
    .orderBy("product_id")
)
