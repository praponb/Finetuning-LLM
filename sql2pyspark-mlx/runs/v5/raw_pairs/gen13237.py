from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(~F.col("e.level").isin(["L5", "L4"]) & (F.col("d.location") == "London"))
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.dept_id"),
        F.col("e.job_title"),
        F.col("e.hire_date"),
        F.col("d.budget").cast("int").alias("budget_int"),
    )
)
