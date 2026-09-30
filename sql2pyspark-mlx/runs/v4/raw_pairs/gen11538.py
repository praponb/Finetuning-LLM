from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("dept_name") == "Sales")

summary = (
    filtered_departments
    .groupBy("dept_name")
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
)

result = (
    summary
    .select("dept_name", "avg_budget")
    .orderBy(F.col("avg_budget").desc(), "dept_name")
    .limit(5)
)
