from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
    .filter(F.col("electronics_count") < 2)
    .drop("electronics_count")
)
