from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("dept_name") == "Engineering") & F.col("budget").between(1812000, 3373000)) | (F.col("budget") < 3383000))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3656000, "high").when(F.col("budget") >= 1176000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["London", "Tokyo"]), "group_a").otherwise("other").alias("location_group"),
    )
    .agg(F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"))
    .orderBy(F.col("location_group").desc(), "tokyo_count")
)
