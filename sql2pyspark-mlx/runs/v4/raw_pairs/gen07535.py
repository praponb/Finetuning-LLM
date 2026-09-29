from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"))
    .orderBy(F.col("fashion_count").desc(), "category")
    .limit(10)
)
