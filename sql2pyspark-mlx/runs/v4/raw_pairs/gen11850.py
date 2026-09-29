from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("id", "location", F.lower("dept_name").alias("dept_name_lower"))
    .orderBy("location")
)
