from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 9)
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)
