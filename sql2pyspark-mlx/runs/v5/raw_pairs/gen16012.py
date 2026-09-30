from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-05-14"))) & F.col("supplier_id").isNotNull())
    .select("*", (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"))
)

result = (
    base
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.brand",
        "a.price",
        "a.cost",
        "a.stock_qty",
        "a.supplier_id",
        F.col("b.supplier_name"),
    )
)
