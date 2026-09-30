from pyspark.sql import functions as F

result = (
    df_employees
    .cube(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
    )
)
