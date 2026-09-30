from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 364, "high").when(F.col("p.stock_qty") >= 52, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg((F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"))
    .orderBy("stock_qty_bucket", "units_range")
    .limit(1)
)
