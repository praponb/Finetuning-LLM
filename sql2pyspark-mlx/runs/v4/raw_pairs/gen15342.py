from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2023-07-18")
    .groupBy("job_title")
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .orderBy(F.col("job_title").desc())
    .limit(5)
)
