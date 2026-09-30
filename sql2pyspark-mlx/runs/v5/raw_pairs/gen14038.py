from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name", "location")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 7)
)
