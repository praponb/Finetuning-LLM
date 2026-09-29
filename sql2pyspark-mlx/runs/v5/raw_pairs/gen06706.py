from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("job_title", F.quarter("hire_date").alias("hire_quarter"), "level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .filter(F.col("salary_range") < 71)
)
