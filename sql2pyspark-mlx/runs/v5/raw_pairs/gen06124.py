from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("location").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("budget").isNull())
    .select(
        "*",
        F.when(F.col("location").isin(["Berlin", "New York"]), "Priority").otherwise("Other").alias("location_group"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") < 145000), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("running_count_budget", F.count("budget").over(w2))
    .withColumn("running_max_budget", F.max("budget").over(w2))
    .withColumn("running_min_budget", F.min("budget").over(w2))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        "s.dept_name_title",
        "s.location_filled",
        "s.running_count_budget",
        "s.running_max_budget",
        "s.running_min_budget",
        F.col("c.salary").alias("employee_salary"),
        F.col("c.job_title"),
    )
)

deduped = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_title",
        "location_filled",
        "running_count_budget",
        "job_title",
    )
)

result = (
    deduped
    .groupBy(
        F.when(F.col("budget") >= 3189000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.max(F.when(F.col("dept_name") == "Marketing", F.col("running_count_budget"))).alias("marketing_max_running_count_budget"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
    .filter(F.col("marketing_max_running_count_budget") > 0)
    .orderBy("distinct_dept_names")
)
