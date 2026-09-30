from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 26)
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
        F.countDistinct("salary").alias("unique_managers"),
    )
    .filter(F.col("unique_managers") < 4)
    .drop("unique_managers")
    .orderBy("l4_count")
)
