from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-14").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

result = (
    cleaned
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.level_filled",
        "e.level_clean",
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.level"),
    )
)
