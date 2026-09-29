from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())

latest = (
    df_departments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_name", "location")
)

eligible = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date").between("2023-07-01", "2025-02-17")), "left_semi")
)

eligible_departments = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.email").like("%@gmail.com")), "left_anti")
)

not_flagged = (
    eligible_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") >= "2025-04-01"), "left_anti")
)

base = (
    not_flagged
    .filter((~F.col("dept_name").eqNullSafe(F.lit("Sales"))) & F.col("dept_name").isin(["Sales", "HR", "Marketing"]))
    .withColumn("location_lower", F.lower("location"))
)

cleaned = (
    base
    .filter(~F.col("location").isin(["Bangkok", "Berlin", "New York"]))
    .select("*", F.length("location").alias("location_length"))
)

result = (
    cleaned
    .withColumn("location_length_tier", F.when(F.col("location_length") >= 81, "LARGE").otherwise("SMALL"))
    .rollup("location_length_tier", "dept_name")
    .agg(
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.percentile_approx("location_length", 0.95).alias("location_length_p95"),
        F.countDistinct("location").alias("distinct_locations"),
        F.grouping_id().alias("grouping_id"),
    )
)
