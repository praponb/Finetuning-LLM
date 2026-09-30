from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date_plus_59d").desc_nulls_first(), "id")
w_running = Window.partitionBy("manager_id", "dept_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date_plus_59d", "id")

filtered = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-03-28")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.concat_ws("-", "email", "job_title").alias("email_job_title_key"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "level",
        "email_job_title_key",
        F.date_add("hire_date", 59).alias("hire_date_plus_59d"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.first("level", ignorenulls=True).over(w_running).alias("first_known_level"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

remaining_keys = (
    scored
    .select("id", "dept_id")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L3", "L2", "L4"]))
        .select("id", "dept_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube("level")
    .agg(
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50_2"),
        F.grouping("level").alias("gid"),
    )
    .filter(F.col("num_levels") >= 1)
    .orderBy(F.col("row_seq_p50_2").desc_nulls_first())
)
