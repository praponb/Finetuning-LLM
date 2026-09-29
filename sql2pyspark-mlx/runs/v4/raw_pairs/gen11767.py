from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .select("location", "dept_name")
    .distinct()
)
