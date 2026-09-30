from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2206000) & (F.col("budget") > 1293000))
    .select("location")
    .orderBy("location")
)
