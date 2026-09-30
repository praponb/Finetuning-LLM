from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2025-02-08")
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.avg("salary").alias("avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
