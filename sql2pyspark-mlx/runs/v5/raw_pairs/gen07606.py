from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2534000)
    .select("id", "dept_name", "budget", "location")
    .orderBy(F.col("location").desc())
)
