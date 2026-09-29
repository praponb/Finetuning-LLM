from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
    .filter(F.col("electronics_count") >= 8)
    .orderBy(F.col("electronics_count").desc())
)
