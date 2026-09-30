from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(F.col("location").isin(["London", "Tokyo", "Berlin"]) & F.col("location").isin(["Chiang Mai", "New York", "Tokyo"]))
    .withColumn("location_length", F.length("location"))
)

summary = (
    recent_departments
    .groupBy(
        F.when(F.col("location").isin(["Chiang Mai", "Tokyo"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.count("budget").alias("budget_count"),
    )
)

result = (
    summary
    .select("location_group", "budget_count")
    .orderBy(F.col("budget_count").desc())
)
