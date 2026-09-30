from pyspark.sql import functions as F

avg_budget = df_departments.filter(F.col("budget") >= 2992000).agg(F.avg("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") >= avg_budget)
    .select("id", "budget", "dept_name", "location")
    .orderBy(F.col("id").desc())
)
