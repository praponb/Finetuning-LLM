from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
        F.count("*").alias("num_products"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("pbs.pct_umbrella"),
        F.col("pbs.num_products"),
    )
    .orderBy("supplier_name")
)
