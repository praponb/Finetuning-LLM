from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.max_cost") > 560)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("pbs.max_cost"),
        F.col("pbs.avg_stock_qty"),
    )
)
