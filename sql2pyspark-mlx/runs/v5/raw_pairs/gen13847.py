from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .groupBy(
        F.month(F.col("s.opened_date")).alias("opened_month"),
        F.when(F.col("s.region").isin(["North", "West"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .orderBy(F.col("opened_month").desc(), "region_group")
    .limit(5)
)
