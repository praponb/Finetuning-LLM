from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.email").like("%@mail.co.th"))
    .select(F.col("d.dept_name"), F.col("e.hire_date"))
    .orderBy("dept_name", "hire_date")
)
