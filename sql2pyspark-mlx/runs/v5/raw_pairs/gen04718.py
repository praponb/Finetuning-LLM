from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2636000)
    .select("id", "budget", "location")
)
