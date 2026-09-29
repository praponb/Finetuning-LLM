from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-03-18")))
    .withColumn("hire_date_period", F.date_trunc("YEAR", "hire_date"))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

valid_employees = (
    valid
    .filter((F.col("hire_date") >= F.lit("2023-07-30").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "hire_date_period",
        "level_filled",
        F.lower("job_title").alias("job_title_lower"),
    )
)

kept_keys = (
    valid_employees
    .select("id")
    .intersectAll(
        df_employees
        .filter(F.col("level") != "L2")
        .select("id")
    )
)

reconciled = (
    valid_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("dept_id_count_salary", F.count("salary").over(w))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("salary") >= 136000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
        "dept_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("dept_id_count_salary", [0.25, 0.5, 0.75]).alias("dept_id_count_salary_quartiles"),
        F.percentile_approx("dept_id_count_salary", 0.5).alias("dept_id_count_salary_p50"),
    )
)
