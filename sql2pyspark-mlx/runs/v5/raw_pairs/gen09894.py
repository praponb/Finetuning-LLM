from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Bangkok", "Berlin"])
    .agg(F.sum("budget"))
    .orderBy("dept_name")
)
