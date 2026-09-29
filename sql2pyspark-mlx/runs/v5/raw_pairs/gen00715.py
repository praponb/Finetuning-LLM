from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Legal", "Engineering"]))
    .groupBy("dept_name")
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") > 4)
    .orderBy(F.col("budget_count").desc(), F.col("dept_name").desc())
)
