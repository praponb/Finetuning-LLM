from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 23]))
    .groupBy("job_title", "manager_id")
    .agg(F.min("salary").alias("min_salary"))
    .filter(F.col("min_salary") > 144000)
    .select("job_title", "manager_id", "min_salary")
    .orderBy(F.col("min_salary").desc(), "job_title", "manager_id")
    .limit(1)
)
