from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sum(F.col("p.price")).alias("total_price"),
        F.sum(F.when(F.col("p.category") == "Books", F.col("p.price")).otherwise(0)).alias("books_price"),
    )
    .filter(F.col("total_price") >= 6300)
)
