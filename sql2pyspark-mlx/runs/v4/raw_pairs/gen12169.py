from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.pct_grocery") > 24)
    .select(
        F.col("s.city"),
        F.col("s.rating"),
        F.col("pbs.latest_launch_date"),
        F.col("pbs.pct_grocery"),
    )
    .orderBy("pct_grocery")
)
