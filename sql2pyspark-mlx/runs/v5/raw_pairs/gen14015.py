from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 9)
    .select("city", "cnt")
    .orderBy(F.col("cnt").desc())
)
