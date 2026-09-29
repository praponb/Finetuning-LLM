from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("e.salary"),
        F.col("e.is_remote"),
        F.col("e.job_title"),
        F.when(F.col("e.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy(F.col("budget").desc())
)
