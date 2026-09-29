from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter((F.length(F.col("e.email")) >= 10) & F.col("e.job_title").isin(["Director", "Intern", "Engineer"]))
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("e.email"),
        F.col("e.is_remote"),
        F.col("e.hire_date"),
    )
    .orderBy(F.col("dept_name").desc())
)
