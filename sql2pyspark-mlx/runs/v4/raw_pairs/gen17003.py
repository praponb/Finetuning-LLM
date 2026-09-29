from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
    )
    .orderBy("total_salary")
)
