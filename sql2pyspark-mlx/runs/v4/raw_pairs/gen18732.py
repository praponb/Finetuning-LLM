from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Engineering")
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "New York"])
    .agg(F.count("budget"))
    .orderBy("dept_name")
)
