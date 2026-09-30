from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 4)
    .select("location", "cnt")
    .orderBy(F.col("cnt").desc(), "location")
    .limit(10)
)
