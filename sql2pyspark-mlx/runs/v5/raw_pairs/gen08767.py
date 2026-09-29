from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .select(
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("p.brand"),
    )
    .orderBy(F.col("sale_date").desc())
)
