from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("order_id").isin([18, 21, 33]))
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("t.stddev_unit_price"),
    )
    .orderBy(F.col("supplier_id").desc())
)
