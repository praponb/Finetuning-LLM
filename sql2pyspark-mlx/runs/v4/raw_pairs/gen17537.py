from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@gmail.com"))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"))
)
