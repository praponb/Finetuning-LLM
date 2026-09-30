from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.launch_date") > "2025-11-01")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.launch_date"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        (F.col("p.cost") / F.nullif(F.col("p.stock_qty"), F.lit(0))).alias("cost_per_stock_qty"),
    )
)
