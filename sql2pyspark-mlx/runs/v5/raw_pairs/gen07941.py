from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("job_title").isin(["Manager", "Designer", "Director"]))
    .groupBy(
        F.when(F.col("level").isin(["L5", "L2"]), "group_a").otherwise("other").alias("level_group"),
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
)
