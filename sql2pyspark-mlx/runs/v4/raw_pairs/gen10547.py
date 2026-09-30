from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering", "Marketing"]) | (F.col("budget") >= 2164000))
    .withColumn("location_dept_name_key", F.concat_ws(",", "location", "dept_name"))
)
