from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .pivot("level", ["L4", "L1", "L5"])
    .agg(F.sum("salary").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
