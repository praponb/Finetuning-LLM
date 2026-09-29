from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(
        F.when(F.col("d.budget") >= 4038000, "high").when(F.col("d.budget") >= 1469000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("e.job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.min(F.col("e.salary")).alias("min_salary"),
    )
    .orderBy("budget_bucket")
)
