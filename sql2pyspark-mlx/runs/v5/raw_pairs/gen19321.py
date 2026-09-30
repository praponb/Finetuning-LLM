from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-06-01", "2024-07-01"))
    .select("id", "dept_id", F.initcap(F.trim("job_title")).alias("job_title_title"))
    .orderBy(F.col("dept_id").desc(), F.col("id").desc())
)
