from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .groupBy("level")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy(F.col("level").desc())
)
