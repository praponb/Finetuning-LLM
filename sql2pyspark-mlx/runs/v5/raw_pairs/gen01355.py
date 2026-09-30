from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Intern"]))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("D.l3_count"),
        F.col("D.min_hire_date"),
    )
    .orderBy("dept_name")
)
