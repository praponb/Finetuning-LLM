from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.units") < 16)
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
)
