from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "New York"]))
    .select("budget")
    .orderBy("budget")
)
