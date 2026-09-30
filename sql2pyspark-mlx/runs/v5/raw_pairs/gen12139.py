from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .withColumn("location_dept_name_key", F.concat_ws("-", "location", "dept_name"))
)
