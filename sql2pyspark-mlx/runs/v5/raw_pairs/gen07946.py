from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2178000)
    .orderBy(F.col("id").desc(), "budget")
)
