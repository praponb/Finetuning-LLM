from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Finance", "Sales", "HR"])
    .agg(F.avg("budget"))
    .orderBy("location")
)
