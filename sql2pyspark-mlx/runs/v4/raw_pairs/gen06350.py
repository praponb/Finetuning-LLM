from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-03-16").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        F.dayofweek("hire_date").alias("hire_date_dayofweek"),
        F.when(F.col("salary") >= 143000, "High").when(F.col("salary") >= 54000, "Medium").otherwise("Low").alias("salary_band"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.level"),
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.job_title").alias("employee_job_title"),
    )
)
