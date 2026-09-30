from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("hire_date") <= "2024-07-15")
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ds.avg_salary"),
        F.col("ds.pct_l3"),
    )
    .orderBy("id")
)
