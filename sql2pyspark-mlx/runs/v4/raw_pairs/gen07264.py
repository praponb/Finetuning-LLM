from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "New York") & (F.col("budget") > 2636000) & (F.col("dept_name") == "Engineering"))
    .groupBy("dept_name", "location")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") > 35)
)
