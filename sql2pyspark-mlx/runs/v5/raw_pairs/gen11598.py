from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.supplier_id").isin([4, 20, 37]) & (F.col("ss.store_id") == 5))
    .select(
        F.col("p.price"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
    )
    .orderBy("brand", "launch_date")
)
