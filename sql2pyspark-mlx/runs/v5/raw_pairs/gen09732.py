from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "location", F.floor("budget").alias("budget_whole"))
)
