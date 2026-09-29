from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.revenue").isNull())
    .groupBy(F.col("p.category"))
    .agg(
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
)
