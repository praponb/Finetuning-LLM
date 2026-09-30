from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") >= 7)
    .groupBy("job_title")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.avg("salary").alias("avg_salary"),
    )
    .orderBy("job_title")
)
