from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "London") & (F.col("budget") < 3658000))
    .select("id", "dept_name")
    .orderBy("dept_name")
)
