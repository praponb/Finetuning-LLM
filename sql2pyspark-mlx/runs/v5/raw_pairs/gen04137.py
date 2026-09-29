from pyspark.sql import functions as F

max_budget = df_departments.filter(F.col("dept_name").isin(["Engineering", "HR", "Finance"])).agg(F.max("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") >= max_budget)
    .select("id", "budget")
    .orderBy(F.col("id").desc(), F.col("budget").desc())
)
