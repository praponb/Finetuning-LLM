from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~((F.col("e.salary") > 121000) & F.col("m.job_title").isin(["Designer", "Director"])))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.when(F.col("m.salary") >= 163000, "High").when(F.col("m.salary") >= 59000, "Medium").otherwise("Low").alias("salary_tier"),
    )
    .orderBy(F.col("manager_manager_id").desc())
)
