from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Intern")
    .groupBy(F.month("hire_date").alias("hire_month"), "dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
        F.min("salary").alias("min_salary"),
        F.count("*").alias("num_employees"),
    )
    .orderBy("hire_month")
)
