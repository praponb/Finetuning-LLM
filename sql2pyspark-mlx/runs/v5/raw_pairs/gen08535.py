from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.product_name").like("%mouse%") & F.col("p.supplier_id").isin([18, 24]))
    .select(
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("s.region"),
        F.col("s.city"),
    )
)
