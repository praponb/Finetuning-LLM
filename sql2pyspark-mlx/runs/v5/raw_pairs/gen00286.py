from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") > 75)
)
