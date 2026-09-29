from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("salary") > 69000)
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("unassigned")))
)

employees_stats = (
    src
    .groupBy("is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = employees_stats.select("is_remote", "job_title_values")
