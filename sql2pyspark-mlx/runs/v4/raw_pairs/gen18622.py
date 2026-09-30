from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .groupBy(F.col("p.brand"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
        F.count(F.col("ss.revenue")).alias("revenue_count"),
    )
    .orderBy("last_sale_date", F.col("brand").desc())
    .limit(1)
)
