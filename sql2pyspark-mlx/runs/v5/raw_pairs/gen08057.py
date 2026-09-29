from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("price").between(650, 750))
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("price")).otherwise(0)).alias("acme_price"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.acme_price"),
        F.col("t.total_cost"),
    )
)
