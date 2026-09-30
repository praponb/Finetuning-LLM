from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["New York", "Singapore", "Bangkok", "Tokyo"])
    .agg(F.avg("budget"))
    .orderBy("dept_name")
)
