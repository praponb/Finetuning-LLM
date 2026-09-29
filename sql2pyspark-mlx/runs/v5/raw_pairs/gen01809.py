from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select("id", "location", "budget", F.lower("dept_name").alias("dept_name_lower"))
    .orderBy("dept_name_lower")
)
