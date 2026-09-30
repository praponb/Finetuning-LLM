from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.sale_date") >= "2024-12-01")
    .groupBy(
        F.when(F.col("s.rating") >= 4.0, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(
        F.min(F.col("ss.units")).alias("min_units"),
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .orderBy("country", "rating_bucket")
    .limit(10)
)
