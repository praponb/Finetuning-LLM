from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.location").isin(["Berlin", "London"]))
    .groupBy(
        F.when(F.col("d.budget") >= 3447000, "high").when(F.col("d.budget") >= 1700000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"))
    .orderBy("budget_bucket")
)
