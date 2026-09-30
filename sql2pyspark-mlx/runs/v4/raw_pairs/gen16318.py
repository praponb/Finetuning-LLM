from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Marketing", "Finance"])
    .agg(F.sum("budget"))
)
