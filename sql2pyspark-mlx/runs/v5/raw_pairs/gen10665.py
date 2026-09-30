from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)
