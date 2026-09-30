from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter((~F.col("location").eqNullSafe(F.lit("London"))) & (F.col("dept_name") == "Marketing"))
    .withColumn("location_upper", F.upper("location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("UNKNOWN")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.hire_date"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level") == "L2"), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.hire_date")), "left_semi")
)

daily = (
    kept
    .groupBy(
        F.when(F.col("budget") >= 4152000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "level",
    )
    .agg(
        F.count(F.when(F.col("location") == "London", 1)).alias("london_rows"),
        F.sum("budget").alias("total_budget"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
)

result = (
    daily
    .select(
        "budget_tier",
        "level",
        "london_rows",
        "total_budget",
        "finance_rows",
        "budget_stddev",
    )
    .orderBy("finance_rows")
)
