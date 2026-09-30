from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .pivot("level", ["L1", "L5"])
    .agg(F.sum("salary"))
    .select("dept_id", F.col("L1").alias("l1"), F.col("L5").alias("l5"))
)
