from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.upper(F.trim("full_name")) == "JACK TAYLOR")
        .select("manager_id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "hire_date"))
    .groupingSets(
        [["activity_month", "quarter_start"], ["activity_month"], []],
        "activity_month",
        "quarter_start",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
