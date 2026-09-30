from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .groupBy(
        F.when(F.col("p.category").isin(["Grocery", "Fashion"]), "tier_1").otherwise("other").alias("category_group"),
        F.col("p.brand"),
    )
    .agg(
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
    .filter(F.col("max_revenue") > 2900)
    .orderBy("category_group", "min_sale_date")
)
