from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") == 393) & (F.col("supplier_id") == 28))
    .select(
        "id",
        "stock_qty",
        "category",
        "launch_date",
        F.col("cost").cast("int").alias("cost_int"),
        F.substring("brand", 1, 3).alias("brand_prefix"),
    )
)
