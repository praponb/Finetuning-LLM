from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "HR") | (F.col("budget") >= 3573000))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .orderBy("location")
)
