from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Books", "Home"]))
    .groupBy("supplier_id")
    .agg(F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .filter(F.col("supplier_totals.stark_count") >= 78)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("supplier_totals.stark_count"),
    )
)
