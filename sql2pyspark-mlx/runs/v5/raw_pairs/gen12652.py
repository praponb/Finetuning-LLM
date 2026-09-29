from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("dept_name") == "Engineering") & F.col("location").isin(["Bangkok", "Singapore", "New York"])) | (F.col("budget") < 1199000))
    .select("budget", "dept_name")
    .orderBy("dept_name")
)
