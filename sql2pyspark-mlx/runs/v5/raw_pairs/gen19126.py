from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(~F.col("e.is_remote") & F.col("e.manager_id").isNotNull())
    .select(
        F.col("d.location"),
        F.col("e.email"),
        F.col("e.hire_date"),
        F.col("e.job_title"),
        F.round(F.col("d.budget"), 1).alias("budget_rounded"),
    )
)
