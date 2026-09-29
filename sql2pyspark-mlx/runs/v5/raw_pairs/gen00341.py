from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@company.co"))
    .select("id", "is_remote", "job_title", "manager_id")
    .orderBy(F.col("manager_id").desc())
)
