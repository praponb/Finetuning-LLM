from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 22, 27, 40]))
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.sum("salary").alias("total_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
