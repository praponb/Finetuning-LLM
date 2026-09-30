from pyspark.sql import functions as F

valid_products = df_products.filter(F.array_contains("tags", "eco") | F.col("supplier_id").isNull())

products_stats = (
    valid_products
    .groupBy("category", "brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
    )
)

result = (
    products_stats
    .filter(F.col("initech_price") >= 55)
    .select("category", "brand", "initech_price")
    .orderBy("category")
)
