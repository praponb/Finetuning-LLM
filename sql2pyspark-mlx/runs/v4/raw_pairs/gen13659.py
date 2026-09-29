from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .select(F.col("location").alias("label"))
    .unionByName(df_stores.select(F.col("city").alias("label")))
    .distinct()
    .orderBy(F.col("label").desc())
)
