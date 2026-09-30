from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 33) & F.col("dept_id").isin([12, 15, 20, 37]))
    .rollup("job_title", "level")
    .agg(F.min("salary").alias("min_salary"))
    .orderBy("job_title")
)
