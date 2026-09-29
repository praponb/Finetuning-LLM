from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_levels"))
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("department_stats.unique_levels"),
    )
    .orderBy(F.col("unique_levels").desc(), "id")
)
