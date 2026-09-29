from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") > 80000)
)
