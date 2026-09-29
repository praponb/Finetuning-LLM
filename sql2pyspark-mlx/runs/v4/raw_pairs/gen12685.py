from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("job_title").isin(["Analyst", "Director", "Engineer"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.dept_name") != "Finance")
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.dept_name"))
)
