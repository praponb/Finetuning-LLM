from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Finance"])
    .agg(F.avg("budget"))
    .orderBy("location")
)
