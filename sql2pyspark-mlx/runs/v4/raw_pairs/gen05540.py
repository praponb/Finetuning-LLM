from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Bangkok", "Tokyo", "Singapore"])
    .agg(F.count("budget"))
    .orderBy("dept_name")
)
