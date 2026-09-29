from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)
