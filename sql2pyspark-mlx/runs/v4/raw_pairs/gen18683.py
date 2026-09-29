from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Marketing", "Legal"])
    .agg(F.max("budget").alias("value_sum"))
)
