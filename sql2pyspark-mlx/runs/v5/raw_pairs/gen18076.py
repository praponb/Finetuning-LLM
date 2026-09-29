from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Bangkok", "Tokyo"]))
    .select("dept_name")
)
