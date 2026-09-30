from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 1642000) & (F.col("budget") <= 1390000))
    .select("budget")
    .orderBy(F.col("budget").desc())
)
