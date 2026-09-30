from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.size(F.col("p.tags")) > 1) & (F.col("ss.units") <= 28))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
    .orderBy("id", F.col("sale_date").desc())
)
