from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-11-22").cast("date")) & F.col("job_title").isNotNull() & F.col("manager_id").isin([14, 22, 33, 37]))
    .withColumn("level_lower", F.lower("level"))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

result = (
    valid
    .groupBy("manager_id")
    .pivot("level", ["L5", "L3", "L4"])
    .agg(F.sum("salary"))
)
