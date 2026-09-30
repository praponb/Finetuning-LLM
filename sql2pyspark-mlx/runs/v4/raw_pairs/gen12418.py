from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(~F.col("e.is_remote"))
    .groupBy(
        F.when(F.col("d.budget") >= 3981000, "high").when(F.col("d.budget") >= 1031000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min(F.col("e.hire_date")).alias("first_hire_date"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
    .orderBy(F.col("avg_salary").desc(), "budget_bucket")
    .limit(20)
)
