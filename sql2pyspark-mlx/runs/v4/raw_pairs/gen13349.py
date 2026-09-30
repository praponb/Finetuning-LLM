from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("salary").between(123000, 134000))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.engineer_salary") >= 24)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("ds.engineer_salary"),
    )
)
