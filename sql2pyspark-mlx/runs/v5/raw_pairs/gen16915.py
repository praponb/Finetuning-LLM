from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.units") >= 19) & (F.col("ss.revenue") < 1600))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.coalesce(F.col("i.quantity"), F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("store_id").desc())
)
