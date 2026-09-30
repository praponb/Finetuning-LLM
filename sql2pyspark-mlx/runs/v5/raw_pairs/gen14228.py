from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "London", "Berlin"]) & F.col("location").isin(["Tokyo", "New York"]))
    .select("budget", F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"))
)
