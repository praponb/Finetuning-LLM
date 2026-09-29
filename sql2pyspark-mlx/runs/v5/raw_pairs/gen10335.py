from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Tokyo", "Singapore"]) & F.col("location").isin(["Singapore", "Berlin"]))
    .select("id", "dept_name", "budget", "location")
    .orderBy("location")
)
