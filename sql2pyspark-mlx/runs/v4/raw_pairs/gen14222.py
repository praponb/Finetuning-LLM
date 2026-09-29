from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .groupBy("location")
    .pivot("dept_name", ["HR", "Sales", "Marketing", "Finance"])
    .agg(F.avg("budget"))
    .orderBy("location")
)
