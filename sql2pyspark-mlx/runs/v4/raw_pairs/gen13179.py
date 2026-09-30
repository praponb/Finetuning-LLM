from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("budget") > 1267000)

summary = (
    filtered_departments
    .groupBy("location")
    .agg(
        F.count("budget").alias("budget_count"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
)

result = (
    summary
    .select("location", "engineering_count")
    .orderBy(F.col("engineering_count").desc(), "location")
    .limit(5)
)
