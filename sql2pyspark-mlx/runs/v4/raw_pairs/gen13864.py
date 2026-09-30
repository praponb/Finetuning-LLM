from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 115000)
    .select("job_title", "dept_id", "is_remote")
    .orderBy("job_title")
)
