from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("dept_id").orderBy("salary_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("job_title").orderBy("salary_whole", "id")
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id")

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2024-03-02"))) & (F.upper(F.trim("full_name")) == "JACK TAYLOR"))
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.floor("salary").alias("salary_whole"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "salary_whole",
        "job_title_filled",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

scored = (
    cleaned
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w_running))
    .withColumn("next_salary_whole", F.lead("salary_whole", 1, 0).over(w_running_2))
    .withColumn("next_salary_whole_2", F.lead("salary_whole", 1).over(w3))
)

enriched = (
    scored
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.location"), F.col("d.dept_name"), F.col("d.budget"))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
        F.when(F.col("budget") >= 3366000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_rows"),
    )
    .orderBy(F.col("budget_tier").desc_nulls_last())
)
