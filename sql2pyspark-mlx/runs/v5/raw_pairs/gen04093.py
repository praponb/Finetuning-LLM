from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.min("cost").alias("min_cost"))
    .filter(F.col("min_cost") > 190)
    .select("brand", "min_cost")
)
