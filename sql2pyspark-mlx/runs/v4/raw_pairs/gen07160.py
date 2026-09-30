from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue").isNotNull())
    .groupBy(
        F.when(F.col("p.brand").isin(["Initech", "Umbrella"]), "group_a").otherwise("other").alias("brand_group"),
        F.when(F.col("p.stock_qty") >= 419, "high").when(F.col("p.stock_qty") >= 153, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .filter(F.col("avg_revenue") >= 3400)
    .orderBy("stock_qty_bucket")
)
