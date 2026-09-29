from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"), "is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") < 78000)
    .select("hire_year", "is_remote", "l3_count", "min_salary")
    .orderBy(F.col("hire_year").desc(), "is_remote")
    .limit(50)
)
