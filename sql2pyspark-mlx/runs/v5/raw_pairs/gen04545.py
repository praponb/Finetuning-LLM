from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.sale_date") <= "2023-06-16") | F.col("i.product_id").isin([2, 4, 9, 10]))
    .select(
        F.col("i.quantity"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("ss.revenue"),
    )
)
