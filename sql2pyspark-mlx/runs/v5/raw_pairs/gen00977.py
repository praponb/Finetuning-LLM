from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("is_remote_flag").desc_nulls_last())
w3 = Window.partitionBy("dept_id").orderBy(F.col("is_remote_flag").desc())
w_ordered = Window.partitionBy("level").orderBy("salary")
w5 = Window.orderBy("salary")
w6 = Window.partitionBy("dept_id").orderBy("hire_date_month_end", "id")
w_part = Window.partitionBy("manager_id").orderBy(F.col("rank_in_group").desc(), "id")
w8 = Window.partitionBy("job_title").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("is_remote_flag_percent_rank", "id")
w10 = Window.orderBy("seq_no", "id")

base = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("hire_date") > F.lit("2023-05-09").cast("date")))
    .select(
        "*",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.last_day("hire_date").alias("hire_date_month_end"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.percent_rank().over(w2).alias("is_remote_flag_percent_rank"),
        F.cume_dist().over(w3).alias("is_remote_flag_cume_dist"),
        F.cume_dist().over(w_ordered).alias("salary_cume_dist"),
        F.percent_rank().over(w5).alias("salary_percent_rank"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.row_number().over(w_part).alias("seq_no_2"),
        F.sum("salary_cume_dist").over(w8).alias("running_salary_cume_dist"),
        F.lead("salary_percent_rank", 1).over(w9).alias("next_salary_percent_rank"),
        F.row_number().over(w10).alias("event_seq"),
    )
)

result = (
    metrics
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["event_seq_tier", "dept_id"], ["event_seq_tier"]],
        "event_seq_tier",
        "dept_id",
    )
    .agg(
        F.round(F.avg("next_salary_percent_rank"), 2).alias("avg_next_salary_percent_rank"),
        F.round(F.avg("salary_cume_dist"), 2).alias("avg_salary_cume_dist"),
        F.round(F.avg("is_remote_flag_percent_rank"), 2).alias("avg_is_remote_flag_percent_rank"),
        F.round(F.sum("is_remote_flag_cume_dist") / F.nullif(F.count(F.when(F.col("job_title") == "Engineer", 1)), F.lit(0)), 2).alias("is_remote_flag_cume_dist_per_engineer"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
