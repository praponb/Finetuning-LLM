from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & F.col("location").isin(["New York", "Tokyo"]))
    .groupBy(
        "dept_name",
        F.when(F.col("location").isin(["London", "Singapore"]), "priority").otherwise("other").alias("location_group"),
        "location",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .filter(F.col("pct_berlin") >= 58)
)
