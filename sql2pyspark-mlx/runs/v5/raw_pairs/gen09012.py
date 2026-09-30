from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .pivot("level", ["L2", "L5", "L4"])
    .agg(F.avg("salary").alias("amount"), F.count(F.lit(1)).alias("n"))
)
