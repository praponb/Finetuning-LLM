from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") < 5)
    .orderBy(F.col("row_count").desc(), "region")
    .limit(20)
)
