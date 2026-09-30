from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.countDistinct("salary").alias("unique_depts"),
        F.max("salary").alias("max_salary"),
    )
)
