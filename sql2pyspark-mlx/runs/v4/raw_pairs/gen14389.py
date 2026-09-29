from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 8, 18, 33]))
    .select("id", "full_name", "job_title")
)
