from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") > 11)
    .select("id", "dept_id", "job_title")
    .orderBy(F.col("dept_id").desc())
)
