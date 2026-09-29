from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 1307000) | (F.col("location") != "Tokyo"))
    .select("id", "location", "dept_name", "budget")
)
