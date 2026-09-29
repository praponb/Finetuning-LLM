from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("location", "job_title")
w3 = Window.partitionBy("level", "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("salary", "id")
w6 = Window.partitionBy("dept_name", "job_title").orderBy("job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("budget_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("job_title", "location").orderBy("budget_share", "id")
w9 = Window.partitionBy("location").orderBy("first_known_budget", "id")

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2024-03-06")))
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.length("job_title").alias("job_title_length"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.dept_name"), F.col("b.budget"), F.col("b.location"))
)

with_windows = (
    with_dims
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("cumulative_budget", F.sum("budget").over(w3))
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w4))
    .withColumn("next_job_title_length", F.lead("job_title_length", 1).over(w5))
)

enriched = (
    with_windows
    .select(
        "*",
        F.last("budget_share", ignorenulls=True).over(w6).alias("last_known_budget_share"),
        F.sum("budget_share").over(w7).alias("cumulative_budget_share"),
        F.ntile(10).over(w8).alias("budget_share_bucket"),
    )
)

filtered = (
    enriched
    .filter(F.col("budget_share") <= 0.23)
    .select(
        "id",
        "dept_id",
        "salary",
        "is_remote",
        "job_title_length",
        "job_title_filled",
        "budget",
        "location",
        "cumulative_budget",
        "first_known_budget",
        "next_job_title_length",
        "last_known_budget_share",
        "cumulative_budget_share",
        "budget_share_bucket",
        F.round("budget_share", 2).alias("budget_share_rounded"),
        F.round((F.col("salary_share") * 1.15), 2).alias("salary_share_adjusted"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

matched_keys = (
    ranked
    .select("dept_id", "id")
    .intersectAll(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select("dept_id", "id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("next_job_title_length") >= 87, "LARGE").otherwise("SMALL").alias("next_job_title_length_tier"),
        F.when(F.col("last_known_budget_share") >= 0.61, "LARGE").otherwise("SMALL").alias("last_known_budget_share_tier"),
    )
    .agg(
        F.round(F.avg("budget_share_rounded"), 2).alias("avg_budget_share_rounded"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.avg("job_title_length"), 2).alias("avg_job_title_length"),
    )
)
