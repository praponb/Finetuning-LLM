from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") & (F.col("budget") >= 3965000))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("location_title", F.initcap(F.trim("location")))
)
