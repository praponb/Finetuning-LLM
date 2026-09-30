from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("units") != 39)
    .groupBy("product_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.num_store_sales") >= 9)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("ps.num_store_sales"),
    )
    .orderBy("brand", "num_store_sales")
)
