from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("wi%"))
    .groupBy("supplier_id")
    .agg(
        F.count("price").alias("price_count"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("cost")).otherwise(0)).alias("fashion_cost"),
    )
    .filter(F.col("fashion_cost") > 28)
    .orderBy(F.col("supplier_id").desc())
)
