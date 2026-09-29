from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.orderBy("is_remote_flag", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("job_title").orderBy("salary", "id")
w5 = Window.orderBy("is_remote_flag", "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-03-17").cast("date")) & F.col("dept_id").isin([20, 38]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

scored = (
    filtered
    .withColumn("salary_rank", F.rank().over(w))
    .withColumn("salary_bucket", F.ntile(3).over(w2))
    .withColumn("is_remote_flag_avg_2_prior", F.avg("is_remote_flag").over(w3))
    .withColumn("salary_bucket_2", F.ntile(5).over(w4))
    .withColumn("prev_is_remote_flag", F.lag("is_remote_flag", 2).over(w5))
)

base = (
    scored
    .filter(F.col("dept_id") == 24)
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "resolved_level",
        "salary_rank",
        "salary_bucket",
        "is_remote_flag_avg_2_prior",
        "salary_bucket_2",
        "prev_is_remote_flag",
        F.substring("job_title", 1, 3).alias("job_title_prefix"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.budget"))
)

result = (
    joined
    .rollup("dept_id", "manager_id")
    .agg(
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
        F.percentile_approx("salary_bucket", [0.25, 0.5, 0.75]).alias("salary_bucket_quartiles"),
    )
    .orderBy(F.col("distinct_dept_ids").desc())
)
