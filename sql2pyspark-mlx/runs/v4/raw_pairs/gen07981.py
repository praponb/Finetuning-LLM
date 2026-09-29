from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(
        F.when(F.col("d.budget") >= 3093000, "high").when(F.col("d.budget") >= 1352000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max(F.col("e.salary")).alias("max_salary"),
        F.max(F.col("e.hire_date")).alias("latest_hire_date"),
    )
    .orderBy("max_salary", "latest_hire_date", "budget_bucket")
    .limit(3)
)
