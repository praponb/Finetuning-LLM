from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .select("dept_name", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)
