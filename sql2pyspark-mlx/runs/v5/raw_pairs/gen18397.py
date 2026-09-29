from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_id", "location").orderBy("is_remote_flag", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("is_remote_flag", "id")
w3 = Window.partitionBy("location").orderBy(F.col("is_remote_flag").desc(), "id")
w4 = Window.orderBy("last7_count_is_remote_flag", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("location", "job_title")

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L3"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .filter(F.col("p.location") != "Bangkok")
    .select(
        "s.id",
        "s.full_name",
        "s.email",
        "s.dept_id",
        "s.job_title",
        "s.is_remote",
        "s.level",
        "s.is_remote_flag",
        "s.level_filled",
        F.col("p.location"),
        F.col("p.dept_name"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "email",
        "job_title",
        "is_remote",
        "is_remote_flag",
        "location",
        F.min("is_remote_flag").over(w_ordered).alias("last7_min_is_remote_flag"),
        F.avg("is_remote_flag").over(w_ordered).alias("last7_avg_is_remote_flag"),
        F.count("is_remote_flag").over(w_ordered).alias("last7_count_is_remote_flag"),
        F.ntile(4).over(w2).alias("is_remote_flag_bucket"),
    )
)

base = (
    enriched
    .filter(F.col("job_title").isNotNull() & F.col("last7_min_is_remote_flag").isNotNull())
    .select(
        "*",
        F.round("last7_count_is_remote_flag", 0).alias("last7_count_is_remote_flag_rounded"),
        (F.col("last7_count_is_remote_flag") - F.col("is_remote_flag")).alias("last7_count_is_remote_flag_minus_is_remote_flag"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.ntile(4).over(w3).alias("is_remote_flag_bucket_2"),
        F.avg("last7_count_is_remote_flag_rounded").over(w4).alias("last7_count_is_remote_flag_rounded_avg_2"),
        F.sum("is_remote_flag_bucket").over(w5).alias("location_sum_is_remote_flag_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("is_remote_flag_bucket_2") >= 9, "PREMIUM").otherwise("STANDARD").alias("is_remote_flag_bucket_2_tier"),
    )
    .agg(
        F.percentile_approx("last7_avg_is_remote_flag", [0.25, 0.5, 0.75]).alias("last7_avg_is_remote_flag_quartiles"),
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.round(F.sum("last7_count_is_remote_flag_rounded_avg_2") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("last7_count_is_remote_flag_rounded_avg_2_per_tokyo"),
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("last7_count_is_remote_flag_rounded_avg_2")).otherwise(0)).alias("chiang_mai_last7_count_is_remote_flag_rounded_avg_2"),
    )
    .orderBy(F.col("is_remote_flag_bucket_2_tier").asc_nulls_first())
)
