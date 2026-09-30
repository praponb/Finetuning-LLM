from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") < 67000)
    .orderBy(F.col("avg_salary").desc())
)
