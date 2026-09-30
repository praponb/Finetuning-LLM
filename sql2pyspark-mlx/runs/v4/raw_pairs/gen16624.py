from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.brand").isin(["Acme", "Umbrella"]))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("p.stock_qty"))
    .orderBy(F.col("stock_qty").desc(), "quantity")
)
