from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%FRANK%") & F.month("hire_date").isin([6, 7, 8]))
    .select("id", "dept_id", "email", "job_title")
)
