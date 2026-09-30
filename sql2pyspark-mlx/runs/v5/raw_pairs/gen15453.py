from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(((F.col("dept_name") != "Engineering") & ~F.col("location").isin(["Singapore", "Berlin", "Chiang Mai"])) | (F.col("location") == "Singapore"))
)

summary = (
    recent_departments
    .groupBy("location")
    .agg(F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"))
)

result = summary.orderBy("location")
