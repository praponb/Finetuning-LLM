from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("dept_id").isin([7, 8, 21, 32]))
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.max_salary") >= 143000)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("t.max_salary"),
        F.col("t.manager_count"),
    )
    .orderBy(F.col("manager_count").desc())
)
