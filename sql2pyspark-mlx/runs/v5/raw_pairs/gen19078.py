from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Marketing", "Engineering", "HR", "Legal"])
    .agg(F.count("budget"))
    .orderBy("location")
)
