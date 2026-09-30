from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id", "level").orderBy(F.col("email_length").desc_nulls_last(), "id")
w3 = Window.partitionBy("manager_id", "level")
w4 = Window.orderBy(F.col("email_length").desc_nulls_last())

valid = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
        F.length("email").alias("email_length"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .filter(F.col("full_name").isNull())
    .select(
        "id",
        "dept_id",
        "job_title",
        "is_remote",
        "full_name_normalized",
        "email_length",
        "job_title_filled",
        "latest_rank",
        F.stddev_samp("email_length").over(w3).alias("manager_id_stddev_samp_email_length"),
        F.percent_rank().over(w4).alias("email_length_percent_rank"),
    )
)

result = (
    with_windows
    .rollup("dept_id")
    .agg(
        F.avg(F.when(F.col("job_title") == "Director", F.col("email_length"))).alias("director_avg_email_length"),
        F.percentile_approx("email_length", [0.25, 0.5, 0.75]).alias("email_length_quartiles"),
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.avg("email_length"), 2).alias("avg_email_length"),
        F.grouping("dept_id").alias("grouping_id"),
    )
    .orderBy(F.col("director_avg_email_length").desc_nulls_last())
)
