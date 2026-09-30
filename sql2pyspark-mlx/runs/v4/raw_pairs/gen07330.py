from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 2625000) & (F.col("location") != "Tokyo"))
    .select("id", "dept_name", "budget", "location")
    .orderBy("dept_name")
)
