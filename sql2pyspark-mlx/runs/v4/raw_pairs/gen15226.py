from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("is_remote") & F.col("is_remote"))
    .withColumn("is_designer", F.when(F.col("job_title") == "Designer", 1).otherwise(0))
)

agg = (
    filtered_employees
    .groupBy("level")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    agg
    .select("level", "last_hire_date", "min_hire_date")
    .orderBy(F.col("level").desc())
    .limit(10)
)
