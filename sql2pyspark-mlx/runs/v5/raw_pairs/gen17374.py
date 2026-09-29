from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.category",
        "a.brand",
        "a.cost",
        "a.stock_qty",
        "a.supplier_id",
        F.col("p.rating"),
        F.col("p.supplier_name"),
    )
)
