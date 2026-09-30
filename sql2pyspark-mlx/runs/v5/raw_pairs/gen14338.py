from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .groupBy("brand")
    .agg(
        F.count("*").alias("num_products"),
        F.count("price").alias("price_count"),
    )
    .filter(F.col("num_products") >= 5)
    .orderBy(F.col("num_products").desc(), "brand")
)
