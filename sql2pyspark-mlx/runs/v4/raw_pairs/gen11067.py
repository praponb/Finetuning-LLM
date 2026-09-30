from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") > 2300)
    .select("brand", "total_cost")
    .orderBy("brand")
    .limit(1)
)
