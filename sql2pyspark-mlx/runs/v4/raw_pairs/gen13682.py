from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("WEEK", "hire_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .filter(F.col("l5_count") >= 1)
)
