from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.cost") < 590)
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("p.category") == "Books", F.col("p.stock_qty")).otherwise(0)).alias("books_stock_qty"),
        F.avg(F.col("p.stock_qty")).alias("avg_stock_qty"),
    )
    .orderBy("avg_stock_qty")
)
