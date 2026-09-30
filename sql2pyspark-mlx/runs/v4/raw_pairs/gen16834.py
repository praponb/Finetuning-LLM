from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([2, 6, 17]) & ((F.col("level") != "L4") | F.col("email").like("%@company.co")))
    .select("id", "hire_date", "email", "job_title")
)
