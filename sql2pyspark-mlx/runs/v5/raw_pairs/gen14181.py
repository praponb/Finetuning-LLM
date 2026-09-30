from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.budget").between(2693000, 3884000))
    .groupBy(
        F.when(F.col("d.budget") >= 3954000, "high").when(F.col("d.budget") >= 1693000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.avg(F.col("e.salary")).alias("avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
