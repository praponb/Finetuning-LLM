from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("level").isin(["L4", "L2", "L1"]) & (F.col("salary") < 100000) & F.col("dept_id").isin([23, 24]))
)

agg = (
    src
    .groupBy(
        F.when(F.col("salary") >= 126000, "high").when(F.col("salary") >= 56000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    agg
    .select("salary_bucket", "job_title_list")
    .orderBy("salary_bucket")
)
