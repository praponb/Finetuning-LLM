from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering"]) & F.col("dept_name").isNull())
    .select("id", "budget", "dept_name", "location")
)
