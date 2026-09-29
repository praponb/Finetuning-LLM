from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("location") != "Bangkok") & (F.col("dept_name") == "Sales"))
)

summary = (
    recent_departments
    .groupBy("location")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
)

result = (
    summary
    .filter(F.col("budget_range") >= 51)
    .select("location", "budget_range")
)
