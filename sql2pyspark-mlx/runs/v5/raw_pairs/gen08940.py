from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([3, 10, 11]) & F.col("price").between(320, 990))
    .groupBy(
        F.when(F.col("cost") >= 590, "high").when(F.col("cost") >= 350, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.count("*").alias("row_count"),
    )
)
