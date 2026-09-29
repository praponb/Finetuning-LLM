from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("p.cost"),
    )
)
