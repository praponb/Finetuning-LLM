from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.col("manager_id").isNull())

summary = (
    recent_employees
    .groupBy(
        F.month("hire_date").alias("hire_month"),
        F.year("hire_date").alias("hire_year"),
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    summary
    .filter(F.col("salary_range") > 26)
    .select("hire_month", "hire_year", "stddev_salary", "salary_range")
    .orderBy("stddev_salary", F.col("hire_year").desc())
)
