from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.lower(F.trim("email")) == "bob.smith@yahoo.com") & (F.col("manager_id") == 2))
    .select("job_title", "hire_date")
)
