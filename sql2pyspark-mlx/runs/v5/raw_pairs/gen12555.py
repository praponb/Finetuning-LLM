from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .filter(F.col("p.launch_date").between("2023-11-01", "2025-10-01") & (F.col("p.launch_date") < "2025-06-01"))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("p.stock_qty"))
    .orderBy("quantity")
)
