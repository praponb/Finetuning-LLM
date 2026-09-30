from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Singapore"]) & F.col("dept_name").isin(["Finance", "Engineering"]) & F.col("location").isin(["Tokyo", "Singapore"]))
    .select("id", "budget", "location", "dept_name")
)
