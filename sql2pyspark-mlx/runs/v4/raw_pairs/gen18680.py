from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("budget").between(1962000, 3283000) & ~F.col("location").isin(["Berlin", "Singapore", "London"])) | F.col("location").isin(["Tokyo", "Singapore"])))
    .select("id", "dept_name", F.length("location").alias("location_length"))
)
