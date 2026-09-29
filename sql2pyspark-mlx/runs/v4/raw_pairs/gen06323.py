from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.max("salary").alias("max_salary"))
    .orderBy("job_title")
)
