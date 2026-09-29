from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_upper", F.upper("location"))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

result = (
    cleaned
    .groupBy("dept_name")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .filter(F.col("cnt") < 1)
    .orderBy("dept_name")
)
