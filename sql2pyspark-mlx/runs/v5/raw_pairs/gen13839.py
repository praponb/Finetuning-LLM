from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("cost") > 370)
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.stddev_stock_qty") > 25)
    .select(F.col("s.rating"), F.col("ss.stddev_price"), F.col("ss.stddev_stock_qty"))
)
