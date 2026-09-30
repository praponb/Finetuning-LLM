from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .orderBy(F.col("manager_id").desc())
    .limit(5)
)
