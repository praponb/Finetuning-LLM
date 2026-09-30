from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 2204000) & (F.col("location") == "Singapore") & (F.col("location") == "Chiang Mai"))
    .select("id", "dept_name")
    .orderBy("id")
)
