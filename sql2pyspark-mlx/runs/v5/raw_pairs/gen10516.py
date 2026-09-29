from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") >= "2023-03-12") & F.col("full_name").like("%For%"))
    .agg(
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
        F.min("hire_date").alias("min_hire_date"),
    )
)
