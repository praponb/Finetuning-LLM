from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stddev_cost") >= 29)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("t.stddev_cost"))
)
