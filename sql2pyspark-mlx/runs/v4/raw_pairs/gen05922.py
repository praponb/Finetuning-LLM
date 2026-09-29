from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") >= 268) | F.col("supplier_id").isNotNull())
    .select(
        "id",
        "price",
        "stock_qty",
        "category",
        F.when(F.col("category") == "Electronics", 1).otherwise(0).alias("is_electronics"),
        F.upper("category").alias("category_upper"),
    )
)
