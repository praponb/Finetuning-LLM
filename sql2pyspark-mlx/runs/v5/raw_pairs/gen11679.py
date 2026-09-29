from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull() | ~F.col("email").like("%@outlook.com"))
    .select(
        "id",
        "job_title",
        "hire_date",
        "manager_id",
        F.trim("email").alias("clean_email"),
    )
)
