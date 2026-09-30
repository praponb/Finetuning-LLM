from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .groupBy("dept_name")
    .pivot("location", ["Bangkok", "Chiang Mai", "New York"])
    .agg(F.sum("budget"))
    .orderBy("dept_name")
)
