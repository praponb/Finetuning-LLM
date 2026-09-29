from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("id", "dept_name", "budget", "location")
)
