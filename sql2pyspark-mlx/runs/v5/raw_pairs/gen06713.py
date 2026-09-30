from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") > 1200)
    .orderBy("category")
    .limit(20)
)
