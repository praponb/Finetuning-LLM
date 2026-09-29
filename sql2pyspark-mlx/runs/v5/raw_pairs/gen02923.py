from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "dept_id")
    .pivot("level", ["L2", "L4"])
    .agg(F.max("salary").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
