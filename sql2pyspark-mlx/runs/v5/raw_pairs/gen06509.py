from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("P.num_store_sales"),
    )
    .orderBy("id")
)
