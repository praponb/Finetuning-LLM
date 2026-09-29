from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") < 2000)
)
