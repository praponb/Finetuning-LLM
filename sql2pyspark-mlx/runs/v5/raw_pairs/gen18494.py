from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 133000, "high").when(F.col("salary") >= 82000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.max("salary").alias("max_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
