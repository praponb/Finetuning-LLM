from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@company.co"))
    .groupBy("level")
    .agg(F.min("salary").alias("min_salary"))
)
