from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .filter(F.col("p.supplier_id").isNull() & F.col("p.supplier_id").isin([2, 22, 27]))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("p.cost"),
    )
)
