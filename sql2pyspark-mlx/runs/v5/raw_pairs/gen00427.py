from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w_hist = Window.partitionBy("level").orderBy("salary", "id")
w3 = Window.partitionBy("manager_id", "job_title").orderBy("salary", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("job_title").orderBy("manager_id_stddev_samp_salary_adjusted", "id")
w5 = Window.partitionBy("job_title").orderBy(F.col("event_seq").desc(), "id")
w6 = Window.partitionBy("job_title").orderBy("salary", "id")
w7 = Window.partitionBy("job_title").orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("manager_id").orderBy("manager_id_stddev_samp_salary_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("manager_id").orderBy("last_known_event_seq_2", "id")
w10 = Window.orderBy("running_manager_id_stddev_samp_salary_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("job_title").orderBy(F.col("last_known_event_seq_2_bucket").desc_nulls_last(), "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.round((F.col("salary") * 1.07), 2).alias("salary_adjusted"),
        F.trim("job_title").alias("clean_job_title"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "clean_job_title",
        F.stddev_samp("salary_adjusted").over(w).alias("manager_id_stddev_samp_salary_adjusted"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("manager_id_stddev_samp_salary_adjusted").over(w3).alias("manager_id_stddev_samp_salary_adjusted_avg_5_prior"),
        F.lead("event_seq", 1).over(w4).alias("next_event_seq"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

enriched_employees = (
    enriched
    .filter(~F.col("job_title").isin(["Intern", "Manager", "Director"]))
    .select(
        "*",
        F.ntile(5).over(w6).alias("salary_bucket"),
        F.last("event_seq_2", ignorenulls=True).over(w7).alias("last_known_event_seq_2"),
        F.sum("manager_id_stddev_samp_salary_adjusted").over(w_running).alias("running_manager_id_stddev_samp_salary_adjusted"),
    )
)

metrics = (
    enriched_employees
    .select(
        "id",
        "job_title",
        "clean_job_title",
        "event_seq_2",
        "last_known_event_seq_2",
        F.ntile(4).over(w9).alias("last_known_event_seq_2_bucket"),
        F.sum("salary").over(w10).alias("running_salary"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w11))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "job_title",
        "last_known_event_seq_2",
        "last_known_event_seq_2_bucket",
        F.when(F.col("job_title").rlike("^(Engineer|Analyst)$"), "GROUP_B").when(F.col("job_title").rlike("^(Manager|Director)$"), "PRIORITY").otherwise("GROUP_A").alias("job_title_zone"),
    )
    .orderBy(F.col("job_title_zone").desc_nulls_last())
)
