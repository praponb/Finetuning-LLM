from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "HR") | (F.col("location") == "Tokyo"))
    .select("id", "dept_name", "budget")
    .orderBy(F.col("id").desc())
)
