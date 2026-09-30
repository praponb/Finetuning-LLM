from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([4, 6, 19, 32]) & (F.col("supplier_id") == 12))
    .select(
        "product_name",
        "tags",
        (F.col("stock_qty") * F.col("price")).alias("stock_qty_times_price"),
    )
)
