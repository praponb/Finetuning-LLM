from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("ps.stddev_revenue"),
        F.col("ps.avg_units"),
    )
    .orderBy("category")
)
