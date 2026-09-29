from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("manager_id") == 18)
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("ds.engineer_count"),
    )
)
