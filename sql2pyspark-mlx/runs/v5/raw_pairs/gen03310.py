from pyspark.sql import functions as F

result = (
    df_departments
    .agg(F.count(F.when(F.col("location") == "London", 1)).alias("london_count"))
    .orderBy("london_count")
)
