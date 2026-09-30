from pyspark.sql import functions as F

result = (
    df_employees
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc())
)
