from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("manager_id") == 35)
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.unique_depts") > 3)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("t.unique_depts"),
        F.col("t.avg_salary"),
    )
    .orderBy(F.col("id").desc())
)
