from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .orderBy("latest_hire_date", "job_title")
)
