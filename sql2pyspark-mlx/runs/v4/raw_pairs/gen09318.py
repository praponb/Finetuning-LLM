from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["London", "Berlin", "Tokyo"])
    .agg(F.avg("budget"))
    .orderBy("dept_name")
)
