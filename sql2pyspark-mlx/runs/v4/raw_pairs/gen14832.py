from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("location") == "New York")

budget_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3516000, "high").when(F.col("budget") >= 788000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = (
    budget_bucket_summary
    .select("budget_bucket", "location_values")
    .orderBy("location_values")
)
