from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 570)
    .select(
        "brand",
        "stock_qty",
        "launch_date",
        F.coalesce("product_name", F.lit("Unknown")).alias("product_name_filled"),
        (F.col("stock_qty") + F.col("cost")).alias("stock_qty_plus_cost"),
    )
)
