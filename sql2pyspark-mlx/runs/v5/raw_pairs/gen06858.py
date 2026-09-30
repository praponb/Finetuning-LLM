from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("category") == "Electronics")
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stddev_cost") >= 35)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("t.avg_cost"),
        F.col("t.stddev_cost"),
    )
    .orderBy(F.col("rating").desc(), F.col("id").desc())
    .limit(1)
)
