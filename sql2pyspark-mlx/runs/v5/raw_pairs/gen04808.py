from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "left")
    .filter(F.col("b.supplier_name").like("%Supply%"))
    .select(
        "a.id",
        "a.product_name",
        "a.price",
        "a.cost",
        "a.launch_date",
        "a.tags",
        "a.supplier_id",
        F.col("b.supplier_name"),
        F.col("b.rating"),
    )
)
