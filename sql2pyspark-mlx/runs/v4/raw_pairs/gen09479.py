from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title", "level").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("dept_id").orderBy("row_seq_2", "id")
w4 = Window.partitionBy("level").orderBy("row_seq_2", "id")
w5 = Window.partitionBy("manager_id").orderBy(F.col("l5_rows").desc())

filtered = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

with_windows = (
    filtered
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("row_seq_2", F.row_number().over(w2))
)

enriched = (
    with_windows
    .select(
        "*",
        F.ntile(4).over(w3).alias("row_seq_2_bucket"),
        F.lead("row_seq_2", 1).over(w4).alias("next_row_seq_2"),
    )
)

base = (
    enriched
    .filter((F.col("hire_date") > F.to_date(F.lit("2024-03-01"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "is_remote_label",
        "row_seq",
        "row_seq_2_bucket",
        "next_row_seq_2",
        F.substring("level", 1, 4).alias("level_prefix"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

cleaned = (
    base
    .filter(F.col("dept_id").isin([23, 24]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        "is_remote_label",
        "row_seq",
        "row_seq_2_bucket",
        "next_row_seq_2",
        "level_prefix",
        "job_title_clean",
        F.round("salary", 1).alias("salary_rounded"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

result = (
    cleaned
    .groupBy(
        "manager_id",
        F.when(F.col("row_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
        F.avg("row_seq").alias("avg_row_seq"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
    )
    .filter(F.col("avg_row_seq") > 0)
    .withColumn("l5_rows_dense_rank", F.dense_rank().over(w5))
)
