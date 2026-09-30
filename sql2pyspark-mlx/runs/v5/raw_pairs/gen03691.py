from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
)
