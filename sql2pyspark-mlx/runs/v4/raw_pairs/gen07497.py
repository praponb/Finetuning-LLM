from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("launch_date").between("2023-07-01", "2024-12-01"))
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.stddev_price"),
        F.col("t.avg_cost"),
    )
)
