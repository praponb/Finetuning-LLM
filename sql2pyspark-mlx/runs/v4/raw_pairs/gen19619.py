from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.units") < 33)
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.floor(F.col("ss.units")).alias("units_whole"),
    )
)
