from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(~F.col("s.store_name").like("S%") & F.col("ss.store_id").isin([2, 4, 5, 6]))
    .select(
        F.col("ss.units"),
        F.col("s.region"),
        F.col("p.cost"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("product_name").desc())
)
