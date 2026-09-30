from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "mia rossi")
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("dt.max_salary"),
    )
    .orderBy("id")
)
