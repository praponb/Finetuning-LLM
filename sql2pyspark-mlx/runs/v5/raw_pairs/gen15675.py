from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws("-", "level", "full_name").alias("level_full_name_key"),
    )
)

filtered_employees = (
    filtered
    .filter((~F.col("level").eqNullSafe(F.lit("L5"))) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "is_remote",
        "level",
        "level_full_name_key",
        F.initcap(F.trim("level")).alias("level_title"),
    )
)

matched_keys = (
    filtered_employees
    .select("manager_id", "level")
    .subtract(
        df_employees
        .filter(F.col("email").like("%@gmail.com"))
        .select("manager_id", "level")
    )
)

kept_rows = (
    filtered_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("manager_id").isNotNull() & F.col("manager_id").isin([18, 33]))
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

result = (
    base
    .groupBy("level")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.count("*").alias("event_count_2"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
    )
    .filter(F.col("num_levels") < 1000)
    .orderBy(F.col("event_count_2").desc_nulls_last())
)
