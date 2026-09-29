from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-04-18").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.concat("product_name", F.lit(", "), "brand").alias("product_name_brand"),
    )
)

result = (
    cleaned
    .groupBy("supplier_id")
    .pivot("category", ["Grocery", "Books", "Home"])
    .agg(F.max("stock_qty").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
