from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 156000, "high").when(F.col("salary") >= 66000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .filter(F.col("l5_count") < 74)
)
