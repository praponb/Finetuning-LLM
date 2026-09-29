from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%MMA%") & (F.length("full_name") > 10))
    .select("is_remote", "job_title")
)
