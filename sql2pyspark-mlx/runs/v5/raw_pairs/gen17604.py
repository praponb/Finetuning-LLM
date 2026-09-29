from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.stock_qty").between(118, 225))
    .groupBy(
        F.when(F.col("s.rating") >= 3.8, "high").when(F.col("s.rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("p.category") == "Books", F.col("p.price")).otherwise(0)).alias("books_price"),
        F.max(F.col("p.launch_date")).alias("last_launch_date"),
    )
)
