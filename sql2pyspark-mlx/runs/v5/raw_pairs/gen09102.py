from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "brand",
        "category",
        "cost",
        F.round((F.col("stock_qty") * 1.1), 2).alias("stock_qty_adjusted"),
    )
    .orderBy("cost", F.col("brand").desc(), "id")
    .limit(5)
)
