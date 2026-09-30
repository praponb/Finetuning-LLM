from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Electronics") & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "launch_date",
        "category",
        F.coalesce("product_name", F.lit("N/A")).alias("product_name_filled"),
        F.round("stock_qty", 2).alias("stock_qty_rounded"),
    )
)
