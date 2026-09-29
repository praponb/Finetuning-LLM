from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("level").isin(["L2", "L1"]))
    .groupBy("dept_id")
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("t.first_hire_date"),
    )
)
