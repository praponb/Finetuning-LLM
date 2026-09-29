from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(-3, -1)
w_hist = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy("days_since_hire_date", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-06-02").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_clean"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "days_since_hire_date",
        "level_clean",
        F.avg("is_remote_flag").over(w).alias("is_remote_flag_avg_3_prior"),
        F.sum("days_since_hire_date").over(w_hist).alias("running_days_since_hire_date"),
        F.sum("days_since_hire_date").over(w3).alias("cumulative_days_since_hire_date"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "salary",
        "is_remote",
        "cumulative_days_since_hire_date",
        F.avg("cumulative_days_since_hire_date").over(w4).alias("last4_avg_cumulative_days_since_hire_date"),
        F.min("cumulative_days_since_hire_date").over(w4).alias("last4_min_cumulative_days_since_hire_date"),
        F.sum("cumulative_days_since_hire_date").over(w4).alias("last4_sum_cumulative_days_since_hire_date"),
    )
)

valid = (
    enriched
    .filter(F.col("cumulative_days_since_hire_date") >= 187)
    .select(
        "*",
        (F.col("salary") * F.col("cumulative_days_since_hire_date")).alias("salary_times_cumulative_days_since_hire_date"),
    )
)

result = (
    valid
    .cube(
        F.when(F.col("salary_times_cumulative_days_since_hire_date") >= 80, "HIGH").otherwise("LOW").alias("salary_times_cumulative_days_since_hire_date_tier"),
        F.when(F.col("last4_min_cumulative_days_since_hire_date") >= 440, "LARGE").otherwise("SMALL").alias("last4_min_cumulative_days_since_hire_date_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last4_sum_cumulative_days_since_hire_date", 0.5).alias("last4_sum_cumulative_days_since_hire_date_p50"),
        F.percentile_approx("last4_sum_cumulative_days_since_hire_date", 0.9).alias("last4_sum_cumulative_days_since_hire_date_p90"),
    )
    .orderBy(
        F.col("salary_times_cumulative_days_since_hire_date_tier").asc_nulls_last(),
    )
)
