from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("is_analyst").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-18").cast("date")) & F.col("job_title").isin(["Manager", "Intern", "Analyst"]))
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("job_title") == "Analyst", 1).otherwise(0).alias("is_analyst"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .filter(~F.col("b.location").isin(["Singapore", "Chiang Mai"]))
    .select("src.*", F.col("b.dept_name"), F.col("b.budget"), F.col("b.location"))
)

valid = (
    joined
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-02-05")))
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "is_analyst",
        "resolved_job_title",
        "dept_name",
        "budget",
        "location",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.upper("location").alias("location_upper"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

cleaned_employees = (
    valid
    .filter(F.col("hire_date") > F.lit("2023-03-21").cast("date"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "is_analyst",
        "resolved_job_title",
        "dept_name",
        "location",
        "is_remote_label",
        "level_filled",
        F.date_format("hire_date", "yyyy-MM-dd").alias("hire_date_label"),
    )
)

ranked = (
    cleaned_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

totals = (
    ranked
    .groupBy(
        F.when(F.col("is_analyst") >= 64, "HIGH").otherwise("LOW").alias("is_analyst_tier"),
    )
    .agg(
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
        F.count("*").alias("num_rows"),
    )
)

result = totals.select("is_analyst_tier", "distinct_dept_ids", "num_rows")
