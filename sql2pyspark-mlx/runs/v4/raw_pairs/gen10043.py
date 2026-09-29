from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 2280000) & F.col("dept_name").isin(["Legal", "HR", "Finance"]))
    .select("id", "budget", F.trim("location").alias("clean_location"))
)
