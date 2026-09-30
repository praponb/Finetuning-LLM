from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "level").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("level").orderBy(F.col("hire_date").asc_nulls_first())
w_running = Window.partitionBy("level").orderBy("salary", "id")
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("level").orderBy("cumulative_salary", "id")

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "full_name",
        "salary",
        "hire_date",
        "level",
        F.sum("salary").over(w_part).alias("cumulative_salary"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .filter(F.col("cumulative_salary").between(222597, 704821))
    .select(
        "*",
        F.lead("event_seq").over(w_running).alias("next_event_seq"),
        F.lag("cumulative_salary").over(w_running).alias("prev_cumulative_salary"),
        F.row_number().over(w_running).alias("row_seq"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4).alias("event_seq_share"),
    )
)

enriched_employees = (
    enriched
    .select(
        "id",
        "full_name",
        "salary",
        "hire_date",
        "level",
        "event_seq",
        "rank_in_group",
        F.lead("row_seq").over(w6).alias("next_row_seq"),
        F.lag("row_seq").over(w6).alias("prev_row_seq"),
        F.lag("event_seq").over(w6).alias("prev_event_seq"),
        F.row_number().over(w6).alias("seq_no"),
        F.row_number().over(w_running).alias("seq_no_2"),
    )
)

matched_keys = (
    enriched_employees
    .select("level", "id")
    .subtract(
        df_employees
        .filter(F.col("level") != "L4")
        .select("level", "id")
    )
)

kept_rows = (
    enriched_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w5)), 4).alias("salary_share"),
    )
)

grouped = (
    metrics
    .withColumn("prev_event_seq_tier", F.when(F.col("prev_event_seq") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["seq_no", "prev_event_seq_tier"], ["seq_no"]],
        "seq_no",
        "prev_event_seq_tier",
    )
    .agg(
        F.count_if(F.col("level") == "L5").alias("l5_events"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary_share")).otherwise(0)).alias("l3_salary_share"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    grouped
    .select(
        "seq_no",
        "prev_event_seq_tier",
        "l5_events",
        "event_count",
        "level_list",
        "l3_salary_share",
        "avg_salary",
    )
    .orderBy(F.col("seq_no").asc_nulls_last())
)
