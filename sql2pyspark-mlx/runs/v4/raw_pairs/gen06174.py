from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Intern")
    .groupBy("is_remote")
    .agg(
        F.min("salary").alias("min_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
    .orderBy("first_hire_date", F.col("min_salary").desc(), "is_remote")
    .limit(5)
)
