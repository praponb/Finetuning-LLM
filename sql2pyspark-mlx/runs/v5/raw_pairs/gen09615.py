from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.col("hire_date") >= "2024-04-01") & (F.col("job_title") == "Manager"))
    .select("id", "dept_id", "level")
    .orderBy("id")
)
