from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.units").between(21, 30))
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.cost") >= 710, "high").when(F.col("p.cost") >= 160, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
)
