from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)
