from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .pivot("level", ["L2", "L3"])
    .agg(F.max("salary"))
    .select("manager_id", F.col("L2").alias("l2"), F.col("L3").alias("l3"))
)
