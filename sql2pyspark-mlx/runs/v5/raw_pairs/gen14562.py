from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2447000)
    .select("location", "dept_name")
    .distinct()
)
