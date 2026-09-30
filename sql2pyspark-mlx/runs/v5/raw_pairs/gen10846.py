from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "New York"]))
    .select("*", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isNotNull()), "left_anti")
)

filtered_departments = (
    kept
    .filter(F.col("dept_name").isNotNull() & (F.col("budget_filled") < 63))
    .select(
        "*",
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

not_flagged = (
    filtered_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.is_remote")), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.job_title").alias("employee_job_title"))
)

daily = (
    with_last
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget_filled")).otherwise(0)).alias("berlin_budget_filled"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") < 1000)
)

result = (
    daily
    .select(
        "location",
        "dept_name_list",
        "num_rows",
        F.when(F.col("location").rlike("^(Tokyo|Chiang Mai)$"), "PRIORITY").when(F.col("location").rlike("^(Bangkok|New York)$"), "EXTENDED").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").asc_nulls_last())
)
