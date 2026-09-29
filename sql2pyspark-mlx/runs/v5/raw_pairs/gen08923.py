from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location").isNotNull() & (F.col("budget") < 3308000)) | F.col("dept_name").isin(["Engineering", "HR"]))
    .select("dept_name", "budget", F.col("budget").cast("int").alias("budget_int"))
    .orderBy("budget")
)
