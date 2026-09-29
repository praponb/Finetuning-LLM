from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Engineering"])
    .agg(F.avg("budget").alias("total"))
)
