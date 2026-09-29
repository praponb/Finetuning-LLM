from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category").isin(["Sports", "Books"]))
    .groupBy("supplier_id")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "inner")
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
        F.col("supplier_summary.avg_cost"),
    )
    .orderBy("city", F.col("avg_cost").desc())
)
