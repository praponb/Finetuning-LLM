from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") & (F.col("dept_name") == "Finance"))
    .select("location", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
    .orderBy("budget_filled")
)
