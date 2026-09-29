from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() | F.col("email").like("%@startup.io"))
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
        F.countDistinct("salary").alias("unique_levels"),
    )
    .filter(F.col("l2_count") > 80)
    .orderBy(F.col("unique_levels").desc())
)
