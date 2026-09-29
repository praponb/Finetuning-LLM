from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-08-13", "2025-05-01"))
    .select("job_title", "level")
    .unionByName(df_employees.filter(F.col("is_remote")).select("job_title", "level"))
    .orderBy("job_title")
)
