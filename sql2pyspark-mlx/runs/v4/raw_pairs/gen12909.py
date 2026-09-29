from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.concat("location", F.lit(" - "), "dept_name").alias("location_dept_name"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.full_name").isNull()), "left_anti")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") < 65000), "left_anti")
)

latest_match = (
    kept_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.hire_date"))
)

result = (
    latest_match
    .groupBy(
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.percentile_approx("budget", 0.9).alias("budget_p90"),
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.countDistinct("location").alias("distinct_locations"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .filter(F.col("budget_stddev") > 0)
    .orderBy("avg_budget")
)
