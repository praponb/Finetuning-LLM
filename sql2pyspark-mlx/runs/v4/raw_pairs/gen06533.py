from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .filter(F.col("s.city").isin(["Berlin", "Chiang Mai"]) & F.col("ss.revenue").between(1500, 3300))
    .select(
        F.col("ss.sale_date"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy(F.col("sale_date").desc())
)
