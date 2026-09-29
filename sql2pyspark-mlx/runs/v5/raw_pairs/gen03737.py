from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "launch_date",
        "supplier_id",
        F.array_contains("tags", "sale").alias("is_sale"),
    )
)

result = (
    valid
    .groupBy("supplier_id")
    .pivot("category", ["Grocery", "Books"])
    .agg(F.avg("price"))
    .select(
        "supplier_id",
        F.col("Grocery").alias("grocery"),
        F.col("Books").alias("books"),
    )
)
