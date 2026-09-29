from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .pivot("level", ["L4", "L1"])
    .agg(F.avg("salary"))
    .select("dept_id", F.col("L4").alias("l4"), F.col("L1").alias("l1"))
)
