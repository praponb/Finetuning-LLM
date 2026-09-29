from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-11-21").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.concat_ws("-", "category", "brand").alias("category_brand_key"),
    )
)

result = (
    cleaned
    .groupBy("supplier_id")
    .pivot("brand", ["Acme", "Stark"])
    .agg(F.max("price"))
)
