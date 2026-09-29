from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .groupBy("supplier_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("cost")).otherwise(0)).alias("initech_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("pbs.total_cost"),
        F.col("pbs.initech_cost"),
    )
    .orderBy("supplier_name", "id")
    .limit(5)
)
