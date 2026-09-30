from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Acme") & ((F.col("stock_qty") <= 131) | (F.col("stock_qty") <= 210)))
    .select(
        "stock_qty",
        "category",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
    )
)
