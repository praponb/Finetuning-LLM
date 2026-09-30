from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .groupBy("manager_id", "job_title")
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("salary_range") < 52)
)
