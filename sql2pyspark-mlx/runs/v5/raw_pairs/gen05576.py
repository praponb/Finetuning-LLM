from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .cube("level", F.month("hire_date").alias("hire_month"))
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
    .orderBy(F.col("avg_salary").desc())
)
