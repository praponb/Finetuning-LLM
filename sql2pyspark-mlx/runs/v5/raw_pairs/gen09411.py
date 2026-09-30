from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .select("location", F.ceil("budget").alias("budget_whole"))
    .orderBy("location")
)
