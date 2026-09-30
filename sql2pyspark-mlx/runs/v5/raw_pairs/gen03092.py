from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 65000)
    .select("id", "manager_id", "job_title")
    .unionByName(
        df_employees
        .filter(F.col("manager_id") == 10)
        .select("id", "manager_id", "job_title")
    )
    .distinct()
)
