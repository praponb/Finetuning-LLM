from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2672000)
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "Tokyo", "Berlin", "Bangkok"])
    .agg(F.sum("budget"))
    .orderBy("dept_name")
)
