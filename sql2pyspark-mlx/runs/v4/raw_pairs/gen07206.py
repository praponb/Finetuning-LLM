from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .select(
        "*",
        F.concat("dept_name", F.lit("/"), "location").alias("dept_name_location"),
        F.coalesce("location", F.lit("NONE")).alias("resolved_location"),
    )
)

result = (
    valid
    .groupBy("dept_name", "location")
    .agg(
        F.avg(F.when(F.col("location") == "Tokyo", F.col("budget"))).alias("tokyo_avg_budget"),
        F.avg("budget").alias("avg_budget"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
    )
)
