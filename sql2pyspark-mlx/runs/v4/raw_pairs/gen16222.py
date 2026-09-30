from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1473000) & (F.col("dept_name").isin(["Sales", "Engineering", "Legal"]) | (F.col("location") != "Chiang Mai")))
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.min("budget").alias("min_budget"),
        F.count("*").alias("department_count"),
    )
    .orderBy(F.col("department_count").desc())
)
