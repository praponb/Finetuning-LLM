from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("launch_date") <= "2023-03-16")
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.pct_stark") >= 66)
    .select(
        F.col("s.rating"),
        F.col("s.city"),
        F.col("pbs.avg_price"),
        F.col("pbs.pct_stark"),
    )
)
