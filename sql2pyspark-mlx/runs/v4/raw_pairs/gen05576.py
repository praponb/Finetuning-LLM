from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category") == "Grocery")
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("cnt"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("S.cnt"),
        F.col("S.price_range"),
    )
    .orderBy("cnt", "id")
    .limit(3)
)
