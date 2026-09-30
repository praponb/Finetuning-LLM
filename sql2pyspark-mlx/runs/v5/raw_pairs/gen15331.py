from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_length", F.length("location"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

result = (
    valid
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "Singapore"])
    .agg(F.max("budget").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
