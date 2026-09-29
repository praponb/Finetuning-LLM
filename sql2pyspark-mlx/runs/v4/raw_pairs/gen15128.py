from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")

ranked = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    ranked
    .filter((F.col("hire_date") > F.lit("2023-09-20").cast("date")) & F.col("level").isNotNull() & (F.col("job_title") == "Analyst"))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "rank_in_group",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("email", F.lit("Unknown")).alias("email_filled"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

base = (
    filtered
    .filter((F.col("hire_date") > F.to_date(F.lit("2024-01-20"))) & F.col("manager_id").isNotNull())
    .withColumn("hire_date_label", F.date_format("hire_date", "MMM yyyy"))
)

stats = (
    base
    .groupBy(F.date_trunc("month", "hire_date").alias("activity_month"))
    .agg(
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.percentile_approx("is_remote_flag", [0.25, 0.5, 0.75]).alias("is_remote_flag_quartiles"),
        F.countDistinct("dept_id").alias("distinct_dept_ids_2"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "distinct_dept_ids",
        "salary_quartiles",
        "distinct_manager_ids",
        "is_remote_flag_quartiles",
        "salary_stddev",
    )
)
