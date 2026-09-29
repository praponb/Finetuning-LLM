from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("units") < 30)
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.max_revenue") > 3500)
    .select(
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("ps.max_revenue"),
        F.col("ps.latest_sale_date"),
    )
)
