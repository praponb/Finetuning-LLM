from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering"]))
    .orderBy("location", "id")
    .limit(10)
)
