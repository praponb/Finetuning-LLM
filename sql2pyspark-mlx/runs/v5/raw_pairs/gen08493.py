from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1792000)
    .select("budget")
    .orderBy(F.col("budget").desc())
)
