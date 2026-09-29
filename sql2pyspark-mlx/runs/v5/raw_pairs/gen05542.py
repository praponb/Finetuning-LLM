from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level").orderBy("salary", "id")
w3 = Window.partitionBy("job_title", "level").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title").orderBy(F.col("running_event_seq").desc_nulls_first())
w6 = Window.partitionBy("job_title", "level").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("level").isNotNull() & (~F.col("job_title").eqNullSafe(F.lit("Intern"))))
    .select("*", F.trim("job_title").alias("clean_job_title"))
)

metrics = (
    base
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.sum("seq_no").over(w3).alias("running_sum_seq_no"),
        F.count("seq_no").over(w3).alias("running_count_seq_no"),
        F.avg("seq_no").over(w3).alias("running_avg_seq_no"),
        F.sum("event_seq").over(w4).alias("running_event_seq"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    ranked
    .filter(F.col("level") != "L2")
    .select(
        "*",
        F.count("running_sum_seq_no").over(w6).alias("running_count_running_sum_seq_no"),
        F.max("running_sum_seq_no").over(w6).alias("running_max_running_sum_seq_no"),
        F.sum("running_sum_seq_no").over(w6).alias("running_sum_running_sum_seq_no"),
    )
)

kept_keys = (
    scored
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L4", "L3", "L5"]))
        .select("job_title")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("full_name", "job_title", "running_count_seq_no", "running_avg_seq_no")
    .orderBy(F.col("running_count_seq_no").desc_nulls_first())
)
