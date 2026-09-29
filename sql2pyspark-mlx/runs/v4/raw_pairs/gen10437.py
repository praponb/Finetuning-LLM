from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("s.country") == "UK") & (F.col("i.quantity") != 113))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy("city", F.col("rating").desc())
)
