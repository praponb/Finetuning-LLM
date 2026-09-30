from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "level").orderBy("budget", "id")
w3 = Window.partitionBy("location").orderBy(F.col("seq_no").asc_nulls_first(), "id")
w_running = Window.partitionBy("location", "dept_name").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name", "location").orderBy("seq_no", "id").rowsBetween(-5, Window.currentRow)

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.level"))
)

enriched = (
    with_last
    .filter(~F.col("job_title").isin(["Director", "Engineer"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.lead("budget").over(w2).alias("next_budget"),
        F.lag("budget").over(w2).alias("prev_budget"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name", "location", "seq_no")
)

metrics = (
    deduped
    .select(
        "id",
        "location",
        F.min("seq_no").over(w_running).alias("running_min_seq_no"),
        F.count("seq_no").over(w_running).alias("running_count_seq_no"),
        F.avg("seq_no").over(w_running).alias("running_avg_seq_no"),
        F.avg("seq_no").over(w5).alias("seq_no_avg_5"),
    )
)

result = (
    metrics
    .groupBy(
        "location",
        F.when(F.col("running_count_seq_no") >= 13, "PREMIUM").otherwise("STANDARD").alias("running_count_seq_no_tier"),
        "running_count_seq_no",
    )
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.percentile_approx("running_min_seq_no", 0.9).alias("running_min_seq_no_p90"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
        F.max("running_count_seq_no").alias("max_running_count_seq_no"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
    )
)
