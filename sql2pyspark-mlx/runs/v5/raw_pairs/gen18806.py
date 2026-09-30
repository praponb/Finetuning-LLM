from pyspark.sql import functions as F

joined = (
    df_employees
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.budget"))
)

result = (
    joined
    .groupBy("dept_id")
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_rows"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
    )
    .filter(F.col("l1_rows") > 1)
)
