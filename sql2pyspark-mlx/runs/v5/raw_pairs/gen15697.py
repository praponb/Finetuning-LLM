from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", "dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .filter(F.col("pct_director") > 78)
)
