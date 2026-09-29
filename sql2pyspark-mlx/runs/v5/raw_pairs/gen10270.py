from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 113000)
    .groupBy("is_remote", "job_title")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .orderBy(F.col("level_values").desc(), "is_remote", "job_title")
    .limit(5)
)
