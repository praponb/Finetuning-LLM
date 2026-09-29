from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 15)
    .groupBy("job_title", F.year("hire_date").alias("hire_year"))
    .agg(F.sum("salary").alias("total_salary"))
    .orderBy(F.col("hire_year").desc())
)
