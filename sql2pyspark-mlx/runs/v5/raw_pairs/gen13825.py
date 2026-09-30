from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "employee_job_title").orderBy(F.col("is_engineer").desc())
w_grp = Window.partitionBy("employee_job_title", "job_title").orderBy(F.col("salary").desc())
w3 = Window.orderBy(F.col("salary").desc())
w4 = Window.partitionBy("employee_job_title", "job_title").orderBy("salary")
w5 = Window.partitionBy("job_title").orderBy("salary")
w_running = Window.orderBy(F.col("employee_salary").desc_nulls_last())
w_ordered = Window.orderBy("salary_cume_dist")

valid = (
    df_employees
    .filter((F.col("job_title") == "Designer") & (~F.col("level").eqNullSafe(F.lit("L1"))))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.when(F.col("job_title") == "Engineer", 1).otherwise(0).alias("is_engineer"),
        F.concat_ws("-", "email", "level").alias("email_level_key"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.manager_id",
        "s.job_title",
        "s.salary",
        "s.hire_date",
        "s.level",
        "s.is_engineer",
        "s.email_level_key",
        "s.level_filled",
        F.col("b.salary").alias("employee_salary"),
        F.col("b.job_title").alias("employee_job_title"),
    )
)

remaining_keys = (
    joined
    .select("job_title", "id")
    .subtract(
        df_employees
        .filter(F.col("full_name").like("G%"))
        .select("job_title", "id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("manager_id").isin([10, 16, 29]))
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "level_filled",
        "employee_salary",
        "employee_job_title",
        F.dense_rank().over(w).alias("is_engineer_dense_rank"),
        F.cume_dist().over(w_grp).alias("salary_cume_dist"),
        F.percent_rank().over(w3).alias("salary_percent_rank"),
    )
)

enriched = (
    metrics
    .withColumn("salary_percent_rank_2", F.percent_rank().over(w4))
    .withColumn("salary_rank", F.rank().over(w5))
    .withColumn("salary_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("employee_salary_rank", F.rank().over(w_running))
    .withColumn("salary_cume_dist_cume_dist", F.cume_dist().over(w_ordered))
)

base = (
    enriched
    .filter(F.col("employee_job_title").isNotNull())
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "level_filled",
        "employee_job_title",
        "is_engineer_dense_rank",
        "salary_cume_dist",
        "salary_percent_rank",
        "salary_rank",
        "salary_cume_dist_2",
        "employee_salary_rank",
        "salary_cume_dist_cume_dist",
        (F.col("salary_percent_rank_2") / F.nullif("salary_percent_rank", F.lit(0))).alias("salary_percent_rank_2_per_salary_percent_rank"),
        F.round("salary_percent_rank_2", 1).alias("salary_percent_rank_2_rounded"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("week", "hire_date").alias("week_start"))
    .agg(
        F.round(F.sum("salary_percent_rank_2_per_salary_percent_rank") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("salary_percent_rank_2_per_salary_percent_rank_per_manager"),
        F.avg("salary_cume_dist_cume_dist").alias("avg_salary_cume_dist_cume_dist"),
        F.percentile_approx("employee_salary_rank", 0.5).alias("employee_salary_rank_p50"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
    )
)
