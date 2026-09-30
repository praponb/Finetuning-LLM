from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Marketing"]) & (F.col("dept_name") == "Engineering"))
    .select("id", "budget", F.initcap(F.trim("location")).alias("location_title"))
)
