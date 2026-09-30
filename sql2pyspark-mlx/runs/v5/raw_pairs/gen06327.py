from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("suppliers.country") == "DE")
    .groupBy(F.col("suppliers.id"), F.col("suppliers.country"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.sum(F.when(F.col("products.category") == "Books", F.col("products.cost")).otherwise(0)).alias("books_cost"),
        F.round(F.avg(F.col("products.price")), 2).alias("avg_price"),
    )
    .filter(F.col("avg_price") > 28)
    .orderBy(F.col("product_count").desc())
)
