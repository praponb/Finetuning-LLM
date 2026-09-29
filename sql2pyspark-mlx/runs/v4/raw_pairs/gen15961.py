from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .groupBy("dept_name")
    .pivot("location", ["Bangkok", "Berlin", "Singapore"])
    .agg(F.max("budget"))
)
