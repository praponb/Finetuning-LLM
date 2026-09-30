from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("stock_qty").isNull())
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", F.col("price")).otherwise(0)).alias("fashion_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.fashion_price") > 21)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("ss.fashion_price"),
    )
    .orderBy(F.col("rating").desc())
)
