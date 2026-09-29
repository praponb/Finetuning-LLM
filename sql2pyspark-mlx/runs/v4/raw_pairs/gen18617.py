from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter((F.col("p.stock_qty") == 310) & F.col("s.supplier_name").like("To%"))
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("p.product_name"),
        F.col("p.price"),
    )
)
