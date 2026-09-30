from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["HR", "Sales", "Finance", "Engineering"])
    .agg(F.sum("budget"))
    .orderBy("location")
)
