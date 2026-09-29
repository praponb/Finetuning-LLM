from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("category") == "Grocery")
    .groupBy("supplier_id")
    .agg(
        F.avg("price").alias("avg_price"),
        F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.stark_count") >= 57)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("t.avg_price"),
        F.col("t.stark_count"),
    )
)
