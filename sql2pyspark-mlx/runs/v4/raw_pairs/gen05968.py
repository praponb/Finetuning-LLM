from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("price").alias("price_count"),
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
    )
    .filter(F.col("umbrella_count") > 4)
    .drop("umbrella_count")
    .orderBy(F.col("category").desc())
)
