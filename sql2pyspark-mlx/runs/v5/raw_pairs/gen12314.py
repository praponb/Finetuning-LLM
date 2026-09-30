from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id")
w3 = Window.partitionBy("level").orderBy(F.col("salary_share").desc(), "id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("salary_share").desc())
w5 = Window.orderBy("salary")
w6 = Window.partitionBy("level")
w7 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w8 = Window.partitionBy("job_title")

scored = (
    df_employees
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.first("level", ignorenulls=True).over(w).alias("first_known_level"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

filtered = (
    scored
    .filter((F.col("hire_date") >= F.lit("2023-07-29").cast("date")) & (~F.col("level").eqNullSafe(F.lit("L1"))))
    .select(
        "*",
        F.when(F.col("level").isin(["L2", "L5"]), "Group A").otherwise("Other").alias("level_group"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    latest
    .select("level", "job_title")
    .subtract(
        df_employees
        .filter(F.col("dept_id") == 24)
        .select("level", "job_title")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.month("hire_date").isin([1, 10, 11]))
    .select(
        "id",
        "full_name",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "salary_share",
        "resolved_level",
        "rank_in_group",
        F.percent_rank().over(w4).alias("salary_share_percent_rank"),
        F.dense_rank().over(w5).alias("salary_dense_rank"),
        F.avg("salary").over(w6).alias("level_avg_salary"),
        F.cume_dist().over(w7).alias("salary_cume_dist"),
        F.count("salary").over(w8).alias("job_title_count_salary"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("salary_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("salary_dense_rank_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
    )
    .agg(
        F.avg(F.when(F.col("level") == "L1", F.col("job_title_count_salary"))).alias("l1_avg_job_title_count_salary"),
        F.percentile_approx("level_avg_salary", 0.95).alias("level_avg_salary_p95"),
        F.percentile_approx("salary_dense_rank", [0.25, 0.5, 0.75]).alias("salary_dense_rank_quartiles"),
        F.round(F.sum("salary_dense_rank") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("salary_dense_rank_per_intern"),
    )
    .orderBy(F.col("l1_avg_job_title_count_salary").desc())
)
