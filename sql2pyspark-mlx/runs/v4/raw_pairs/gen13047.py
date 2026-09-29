from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-08-27").cast("date"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.greatest("stock_qty", "cost").alias("greatest_stock_qty_cost"),
        F.substring("category", 1, 4).alias("category_prefix"),
    )
)

base = (
    filtered
    .filter(F.col("price") > 430)
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "greatest_stock_qty_cost",
        "category_prefix",
        F.concat("product_name", F.lit(" - "), "brand").alias("product_name_brand"),
    )
)

kept_keys = (
    base
    .select("id")
    .subtract(
        df_products
        .filter(F.col("price") >= 460)
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "product_name",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        "greatest_stock_qty_cost",
        "category_prefix",
        "product_name_brand",
    )
)
