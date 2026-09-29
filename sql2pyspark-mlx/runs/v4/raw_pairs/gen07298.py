from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "job_title")
    .pivot("level", ["L2", "L3"])
    .agg(F.max("salary").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "manager_id",
        "job_title",
        F.col("L2_value_sum").alias("l2_value_sum"),
        F.col("L2_cnt").alias("l2_cnt"),
        F.col("L3_value_sum").alias("l3_value_sum"),
        F.col("L3_cnt").alias("l3_cnt"),
    )
)
