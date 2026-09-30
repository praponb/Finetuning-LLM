from pyspark.sql import functions as F

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-11-02").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.coalesce("product_name", F.lit("N/A")).alias("product_name_filled"),
    )
)

result = (
    valid
    .select("id", "category")
    .subtract(
        df_products
        .filter(F.col("category") == "Grocery")
        .select("id", "category")
    )
)
