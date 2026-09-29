from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "dept_name")
    .orderBy(F.col("id").desc())
)
