from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Engineer"]))
    .groupBy(
        F.quarter("hire_date").alias("hire_quarter"),
        F.year("hire_date").alias("hire_year"),
    )
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
    )
)
