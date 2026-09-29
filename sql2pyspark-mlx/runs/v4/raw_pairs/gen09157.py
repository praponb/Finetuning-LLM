from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2407000, 2450000) & (F.col("location").isin(["Tokyo", "New York"]) | (F.col("budget") <= 1926000)))
    .select("id", "budget", "dept_name", "location")
)
