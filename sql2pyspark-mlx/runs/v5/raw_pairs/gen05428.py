from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["HR", "Marketing", "Engineering"]) | (F.col("dept_name") == "Legal"))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)
