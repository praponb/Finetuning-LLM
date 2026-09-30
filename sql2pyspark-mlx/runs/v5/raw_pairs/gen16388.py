from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(~F.col("location").eqNullSafe(F.lit("Bangkok")))
    .select(
        "*",
        F.round("budget", 2).alias("budget_rounded"),
        F.floor("budget").alias("budget_whole"),
        F.coalesce("location", F.lit("N/A")).alias("location_clean"),
    )
)

matched_keys = (
    filtered
    .select("dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "Marketing", "Engineering"]))
        .select("dept_name")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id") == 33), "left_semi")
)

eligible_departments = (
    eligible
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id") == 13), "left_anti")
)

result = (
    eligible_departments
    .select(
        "dept_name",
        "location",
        "budget_rounded",
        "budget_whole",
        F.when(F.col("location").rlike("^(Singapore|London)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("location_zone"),
    )
)
