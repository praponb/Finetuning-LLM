from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Finance", "Legal", "Marketing"])
    .agg(F.count("budget"))
)
