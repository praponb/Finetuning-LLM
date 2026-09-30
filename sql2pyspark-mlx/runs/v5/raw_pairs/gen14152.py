from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
    .filter(F.col("salary_range") < 63)
    .orderBy("level")
)
