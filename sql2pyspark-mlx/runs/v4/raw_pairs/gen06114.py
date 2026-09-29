from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("t.min_stock_qty"),
        F.col("t.stddev_cost"),
    )
    .orderBy(F.col("id").desc())
)
