from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-12-23"))) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("salary") >= 154000, "High").when(F.col("salary") >= 49000, "Medium").otherwise("Low").alias("salary_band"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("OTHER")).alias("level_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.last("job_title", ignorenulls=True).over(w).alias("last_known_job_title"),
        F.last("level", ignorenulls=True).over(w).alias("last_known_level"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.dept_name"), F.col("d.location"))
)

stats = (
    with_dims
    .groupBy(F.date_trunc("month", "hire_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("location") == "London").alias("london_events"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "london_events",
        "location_list",
        "hr_count",
        "num_rows",
        "event_count",
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
