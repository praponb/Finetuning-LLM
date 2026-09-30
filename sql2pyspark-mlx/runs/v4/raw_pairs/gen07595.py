from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
    .filter(F.col("stddev_salary") > 40)
)
