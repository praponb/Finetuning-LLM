from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.concat(F.col("p.product_name"), F.lit(" - "), F.col("p.category")).alias("product_name_category"),
    )
)
