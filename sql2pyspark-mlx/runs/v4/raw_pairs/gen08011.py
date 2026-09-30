from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(
        F.when(F.col("d.budget") >= 4368000, "high").when(F.col("d.budget") >= 1972000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
    .orderBy("pct_designer", "budget_bucket")
)
