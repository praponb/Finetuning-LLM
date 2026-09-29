from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")
w_part = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.orderBy(F.col("salary").desc())
w4 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("salary_bucket").desc(), "id")
w6 = Window.orderBy("salary_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("manager_id").orderBy(F.col("salary_bucket").desc(), "id")
w8 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w9 = Window.partitionBy("manager_id")
w_ordered = Window.orderBy("cumulative_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("manager_id").orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("level").orderBy(F.col("seq_no_2").asc_nulls_last(), "id")

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.coalesce("level", F.lit("unassigned")).alias("level_filled"),
        F.concat("full_name", F.lit(" - "), "level").alias("full_name_level"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

metrics = (
    cleaned
    .filter(F.col("dept_id").isin([12, 40]))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "level_filled",
        "job_title_clean",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(4).over(w_part).alias("salary_bucket"),
        F.rank().over(w3).alias("salary_rank"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "full_name",
        "manager_id",
        "level",
        "level_filled",
        "job_title_clean",
        "salary_bucket",
        F.first("job_title", ignorenulls=True).over(w4).alias("first_known_job_title"),
        F.row_number().over(w5).alias("seq_no_2"),
        F.sum("salary_bucket").over(w6).alias("cumulative_salary_bucket"),
        F.row_number().over(w7).alias("event_seq"),
        F.ntile(5).over(w8).alias("salary_bucket_2"),
    )
)

metrics_employees = (
    scored
    .select(
        "id",
        "manager_id",
        "level",
        "level_filled",
        "salary_bucket",
        "seq_no_2",
        "cumulative_salary_bucket",
        "event_seq",
        F.round((F.col("event_seq") / F.sum("event_seq").over(w9)), 4).alias("event_seq_share"),
        F.last("salary_bucket_2", ignorenulls=True).over(w_ordered).alias("last_known_salary_bucket_2"),
        F.sum("salary_bucket").over(w11).alias("running_salary_bucket"),
    )
)

latest = (
    metrics_employees
    .withColumn("dedup_rank", F.row_number().over(w12))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "level",
        "level_filled",
        "salary_bucket",
        "seq_no_2",
        "cumulative_salary_bucket",
        "event_seq_share",
    )
)

cleaned_employees = (
    latest
    .filter(F.col("seq_no_2") < 6)
    .select(
        "*",
        F.substring("level", 1, 3).alias("level_prefix"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

result = (
    cleaned_employees
    .select(
        "id",
        "level",
        "level_filled",
        "salary_bucket",
        "cumulative_salary_bucket",
        "level_prefix",
        "level_clean",
        F.when(F.col("level").rlike("^(L2|L1)$"), "PRIORITY").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("level").desc())
)
