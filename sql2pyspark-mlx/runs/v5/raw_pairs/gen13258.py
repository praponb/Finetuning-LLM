from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count("*").alias("num_employees"),
    )
    .filter(F.col("l3_count") >= 3)
    .orderBy(F.col("l3_count").desc())
)
