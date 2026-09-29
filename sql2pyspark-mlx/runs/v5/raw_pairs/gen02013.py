from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("full_name").like("Dav%") & F.col("full_name").like("%Kanya%")) | F.col("dept_id").isin([13, 19, 40]))
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.count("*").alias("num_employees"),
    )
)
