from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Director")
    .groupBy("level", F.year("hire_date").alias("hire_year"))
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count("*").alias("employee_count"),
        F.min("salary").alias("min_salary"),
    )
)
