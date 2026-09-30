from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") < 7)
    .groupBy("category")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
    .filter(F.col("min_cost") >= 680)
    .select("category", "min_cost", "fashion_count")
    .orderBy(F.col("fashion_count").desc(), "category")
    .limit(10)
)
