from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3555000)
    .groupBy("location")
    .pivot("dept_name", ["Finance", "HR"])
    .agg(F.count("budget"))
    .orderBy("location")
)
