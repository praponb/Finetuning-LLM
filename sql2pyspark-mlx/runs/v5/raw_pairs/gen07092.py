from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("budget").between(2810000, 3220000) & F.col("location").isin(["Singapore", "Tokyo", "Bangkok"]))
    .withColumn("location_title", F.initcap(F.trim("location")))
)

dept_name_summary = (
    filtered_departments
    .groupBy(
        "dept_name",
        F.when(F.col("location").isin(["Singapore", "New York"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = (
    dept_name_summary
    .select("dept_name", "location_group", "avg_budget")
    .orderBy("location_group", "dept_name")
)
