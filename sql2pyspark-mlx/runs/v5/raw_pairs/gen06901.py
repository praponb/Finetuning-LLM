from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.round((F.col("price") / F.col("cost")), 2).alias("margin"),
    )
)

result = (
    valid
    .groupBy("supplier_id")
    .pivot("category", ["Home", "Books"])
    .agg(F.sum("price").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
