from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "London", "Bangkok"])
    .agg(F.max("budget"))
    .orderBy("dept_name")
)
