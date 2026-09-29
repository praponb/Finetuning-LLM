from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3663000)
    .select("location", "dept_name", "budget")
    .orderBy(F.col("budget").desc(), F.col("location").desc())
)
