from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("units") < 30)
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.total_revenue") >= 7000)
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("t.total_revenue"),
    )
)
