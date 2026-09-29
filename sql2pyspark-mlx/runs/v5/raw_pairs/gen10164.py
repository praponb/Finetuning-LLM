from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter((F.upper(F.trim(F.col("e.email"))) == "EMMA@OUTLOOK.COM") | F.col("e.job_title").isin(["Intern", "Manager"]))
    .select(
        F.col("e.id"),
        F.col("e.full_name"),
        F.col("e.dept_id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)
