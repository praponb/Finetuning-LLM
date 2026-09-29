from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-11-01", "2025-05-16"))
    .select("is_remote", "manager_id")
    .orderBy(F.col("manager_id").desc())
)
