from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .select("id", "budget", "location", F.col("budget").cast("int").alias("budget_int"))
    .orderBy("budget_int")
)
