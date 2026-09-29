from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("revenue") <= 2400)
    .groupBy("product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("pt.avg_revenue"),
        F.col("pt.first_sale_date"),
    )
    .orderBy(F.col("avg_revenue").desc())
)
