from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") < 30)
    .orderBy(F.col("avg_salary").desc())
)
