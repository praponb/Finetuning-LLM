from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") >= 2703000)
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.countDistinct(F.col("e.salary")).alias("unique_job_titles"),
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
    .filter(F.col("employee_count") >= 3)
    .orderBy("employee_count", "id", "dept_name")
    .limit(50)
)
