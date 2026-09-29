from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 3685000) & (F.col("location") != "Berlin") & (F.col("dept_name") != "HR"))
    .select("budget", "location", F.round("budget", 1).alias("budget_rounded"))
)
