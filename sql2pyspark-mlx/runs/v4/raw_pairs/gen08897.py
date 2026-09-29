from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
        F.min("salary").alias("min_salary"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 1)
    .drop("cnt")
    .orderBy(F.col("l3_count").desc())
)
