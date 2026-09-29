from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "dept_name", "location", "budget")
    .orderBy(F.col("location").desc(), "id")
    .limit(5)
)
