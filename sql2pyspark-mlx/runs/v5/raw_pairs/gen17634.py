from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "job_title").orderBy("department_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("running_department_budget").desc())

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-09-03").cast("date")) & F.col("is_remote"))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("UNKNOWN")))
)

joined = (
    base
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .filter(F.col("p.budget").between(1641000, 2242000))
    .select("a.*", F.col("p.budget"))
)

joined_employees = (
    joined
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.job_title",
        "s.level",
        "s.budget",
        F.col("d.location"),
        F.col("d.budget").alias("department_budget"),
        F.col("d.dept_name"),
    )
)

scored = (
    joined_employees
    .select(
        "id",
        "full_name",
        "level",
        "dept_name",
        F.sum("department_budget").over(w).alias("cumulative_department_budget"),
        F.sum("department_budget").over(w2).alias("running_department_budget"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

grouped = (
    deduped
    .groupBy("level")
    .agg(
        F.round(F.avg("running_department_budget"), 2).alias("avg_running_department_budget"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.round(F.avg("running_department_budget"), 2).alias("avg_running_department_budget_2"),
    )
)

result = (
    grouped
    .select(
        "level",
        "avg_running_department_budget",
        "engineering_events",
        "avg_running_department_budget_2",
        F.when(F.col("level").rlike("^(L4|L2)$"), "GROUP_A").when(F.col("level").rlike("^(L1|L3)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy(F.col("avg_running_department_budget").asc_nulls_last())
)
