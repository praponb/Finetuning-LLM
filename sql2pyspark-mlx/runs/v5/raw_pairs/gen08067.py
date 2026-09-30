from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.trim("dept_name").alias("clean_dept_name"),
        F.ceil("budget").alias("budget_whole"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level") == "L2"), "left_anti")
)
