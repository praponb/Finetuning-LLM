from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("is_remote_flag").asc_nulls_first())
w2 = Window.partitionBy("job_title").orderBy(F.col("is_remote_flag_adjusted").desc_nulls_last())
w3 = Window.partitionBy("job_title").orderBy("hire_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("job_title_length").desc())
w5 = Window.partitionBy("job_title").orderBy(F.col("hire_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-06").cast("date")) & (F.col("salary") >= 97000))
    .select(
        "*",
        F.lower("level").alias("level_lower"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

filtered = (
    cleaned
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "level_lower",
        "is_remote_flag",
        F.lower("level").alias("level_lower_2"),
        F.trim("full_name").alias("clean_full_name"),
    )
)

valid = (
    filtered
    .filter(F.col("dept_id").isin([10, 14]))
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote_flag",
        "clean_full_name",
        F.date_trunc("MONTH", "hire_date").alias("hire_date_period"),
        F.substring("full_name", 1, 2).alias("full_name_prefix"),
        F.coalesce("level", F.lit("OTHER")).alias("level_filled"),
    )
)

cleaned_employees = (
    valid
    .filter(F.col("job_title").isNotNull() & F.col("dept_id").isin([11, 33]))
    .select(
        "id",
        "job_title",
        "is_remote_flag",
        "hire_date_period",
        "level_filled",
        F.round((F.col("is_remote_flag") * 1.15), 2).alias("is_remote_flag_adjusted"),
        F.length("job_title").alias("job_title_length"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

scored = (
    cleaned_employees
    .filter(F.to_date("hire_date_period") >= "2025-04-01")
    .select(
        "id",
        "job_title",
        "hire_date_period",
        "is_remote_flag_adjusted",
        "job_title_length",
        "job_title_clean",
        F.cume_dist().over(w).alias("is_remote_flag_cume_dist"),
        F.cume_dist().over(w2).alias("is_remote_flag_adjusted_cume_dist"),
        F.last("is_remote_flag", ignorenulls=True).over(w3).alias("last_known_is_remote_flag"),
        F.dense_rank().over(w4).alias("job_title_length_dense_rank"),
        F.avg("is_remote_flag").over(w5).alias("is_remote_flag_avg_30d"),
    )
)

result = (
    scored
    .groupBy("job_title")
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        ((F.unix_timestamp(F.max("hire_date_period")) - F.unix_timestamp(F.min("hire_date_period"))) / 60).alias("span_minutes"),
    )
)
