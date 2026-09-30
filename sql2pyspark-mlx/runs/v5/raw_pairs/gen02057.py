from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.unique_depts") > 6)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ds.unique_managers"),
        F.col("ds.unique_depts"),
    )
    .orderBy(F.col("unique_managers").desc())
)
