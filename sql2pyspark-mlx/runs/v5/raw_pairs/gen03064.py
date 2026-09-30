from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("launch_date") > F.lit("2023-08-17").cast("date")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("price") - F.col("cost")).alias("margin"),
        F.round((F.col("cost") * 1.15), 2).alias("cost_adjusted"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.category",
        "e.brand",
        "e.launch_date",
        "e.tags",
        "e.supplier_id",
        "e.margin",
        F.col("b.supplier_name"),
        F.col("b.city"),
        F.col("b.rating"),
    )
)
