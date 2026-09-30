from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 300)
    .groupBy("brand")
    .agg(F.min("cost").alias("min_cost"))
    .filter(F.col("min_cost") >= 210)
    .select("brand", "min_cost")
    .orderBy(F.col("min_cost").desc())
)
