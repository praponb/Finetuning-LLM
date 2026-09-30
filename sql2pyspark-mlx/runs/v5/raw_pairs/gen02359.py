from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 153000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count_if(F.col("level") == "L4").alias("l4_events"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_rows"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
)
