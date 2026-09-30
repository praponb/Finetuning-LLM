from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.supplier_id").isin([9, 11]) | (F.col("ss.store_id") == 2))
    .select(
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
)
