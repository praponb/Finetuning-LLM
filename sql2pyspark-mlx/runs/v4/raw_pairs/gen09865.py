from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-08-01", "2024-09-15") | F.col("job_title").isin(["Intern", "Designer"]))
    .select(
        "id",
        "manager_id",
        "level",
        "full_name",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
