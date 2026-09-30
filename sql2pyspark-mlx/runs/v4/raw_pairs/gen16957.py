from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(~F.col("e.job_title").isin(["Director", "Manager"]) & (F.col("d.budget") > 1215000))
    .select(
        F.col("d.dept_name"),
        F.col("e.email"),
        F.col("e.level"),
        F.col("e.job_title"),
    )
)
