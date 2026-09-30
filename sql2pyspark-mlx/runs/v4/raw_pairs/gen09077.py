from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("brand") == "Stark"))
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.concat_ws("/", "product_name", "brand").alias("product_name_brand_key"),
        F.upper("product_name").alias("product_name_upper"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "category",
        F.col("price").alias("PRICE"),
        F.col("cost").alias("COST"),
    )
    .unpivot(["id", "category"], ["PRICE", "COST"], "metric", "val")
    .filter(F.col("val").isNotNull())
)
