from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .groupBy("category")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
    )
    .filter(F.col("max_cost") >= 350)
    .select("category", "max_cost", "umbrella_count")
    .orderBy(F.col("max_cost").desc())
)
