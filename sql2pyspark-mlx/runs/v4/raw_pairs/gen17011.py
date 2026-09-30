from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
)
