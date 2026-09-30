from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Finance"]))
    .groupBy("location")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 2)
    .orderBy("location")
)
