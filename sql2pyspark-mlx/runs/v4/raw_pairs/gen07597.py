from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.avg("salary").alias("avg_salary"),
    )
    .orderBy("job_title")
)
