from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("dept_name").isin(["Legal", "Engineering"]))

agg = (
    filtered_departments
    .groupBy("location")
    .agg(F.count("budget").alias("budget_count"))
)

result = agg.orderBy("location")
