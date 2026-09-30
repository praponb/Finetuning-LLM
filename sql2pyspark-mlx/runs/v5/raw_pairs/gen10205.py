from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id").isNotNull() & F.upper("full_name").like("%ENR%")) | F.col("manager_id").isin([2, 13]))
    .select("id", "is_remote", "full_name", "job_title")
    .orderBy(F.col("id").desc(), "full_name")
    .limit(10)
)
