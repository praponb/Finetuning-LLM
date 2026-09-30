from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary_rounded").desc_nulls_last(), "id")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("job_title", "dept_id")
w4 = Window.partitionBy("job_title").orderBy("salary", "id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-11").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.round("salary", 2).alias("salary_rounded"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "dedup_rank",
        F.count("salary").over(w2).alias("level_count_salary"),
        F.stddev_samp("salary").over(w3).alias("job_title_stddev_samp_salary"),
    )
)

with_windows = (
    metrics
    .withColumn("next_salary", F.lead("salary").over(w4))
    .withColumn("prev_job_title_stddev_samp_salary", F.lag("job_title_stddev_samp_salary").over(w4))
    .withColumn("row_seq", F.row_number().over(w4))
)

by_type = (
    with_windows
    .groupBy("dept_id")
    .pivot("job_title", ["Designer", "Director"])
    .agg(F.max("job_title_stddev_samp_salary").alias("amount"))
)

result = (
    by_type
    .select("dept_id", "Designer", "Director")
    .orderBy(F.col("dept_id").desc(), "Director")
)
