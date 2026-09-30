from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & F.month("hire_date").isin([3, 8, 12]))
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
    .orderBy(F.col("level").desc())
    .limit(10)
)
