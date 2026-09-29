from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("job_title").orderBy("budget_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("budget_adjusted", "id")
w4 = Window.partitionBy("job_title", "location").orderBy("budget_adjusted", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("job_title").orderBy("budget_adjusted")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location").eqNullSafe(F.lit("Singapore"))))
    .select("*", F.round((F.col("budget") * 1.1), 2).alias("budget_adjusted"))
)

filtered_departments = (
    filtered
    .filter((~F.col("dept_name").eqNullSafe(F.lit("Marketing"))) & (F.col("budget_adjusted") < 71))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_adjusted",
        F.col("budget_adjusted").cast("int").alias("budget_adjusted_int"),
        F.coalesce("location", F.lit("NONE")).alias("location_clean"),
    )
)

cleaned = (
    filtered_departments
    .filter(F.col("budget") <= 2955000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget_adjusted",
        "budget_adjusted_int",
        "location_clean",
        F.initcap(F.trim("location")).alias("location_title"),
        (F.col("budget_adjusted") + F.col("budget_adjusted_int")).alias("budget_adjusted_plus_budget_adjusted_int"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.location", "s.budget_adjusted", F.col("c.job_title"))
)

metrics = (
    latest_match
    .select(
        "*",
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
        F.lag("budget_adjusted", 2).over(w3).alias("prev_budget_adjusted"),
        F.avg("budget_adjusted").over(w4).alias("budget_adjusted_avg_2_prior"),
        F.cume_dist().over(w5).alias("budget_adjusted_cume_dist"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.upper(F.col("x.full_name")).like("%IA %")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "location",
        "budget_adjusted",
        "job_title",
        "last_known_job_title",
        "budget_adjusted_avg_2_prior",
        "budget_adjusted_cume_dist",
    )
)
