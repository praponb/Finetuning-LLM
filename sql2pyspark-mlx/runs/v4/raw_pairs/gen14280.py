from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-02-29").cast("date")) & F.col("level").isNotNull() & F.col("level").isin(["L1", "L3"]))
    .withColumn("level_full_name_key", F.concat_ws("-", "level", "full_name"))
    .withColumn("clean_full_name", F.trim("full_name"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy("dept_id")
    .pivot("level", ["L4", "L5"])
    .agg(F.avg("salary").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
