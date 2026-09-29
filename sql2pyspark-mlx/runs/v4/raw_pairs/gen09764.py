from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.max("price").alias("max_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("t.stddev_stock_qty"), F.col("t.max_price"))
    .orderBy("country")
)
