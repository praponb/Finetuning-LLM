from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("year_start").orderBy(F.col("num_levels").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Berlin"))))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Legal", "Sales"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.budget", "s.dept_name_group", F.col("c.level"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.dept_name_group",
        "s.level",
        F.col("c.hire_date"),
        F.col("c.salary"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.hire_date")), "left_semi")
)

totals = (
    not_flagged
    .groupBy(
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
        F.when(F.col("budget") >= 3329000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.avg(F.when(F.col("level") == "L5", F.col("salary"))).alias("l5_avg_salary"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .withColumn("num_levels_rank", F.rank().over(w2))
)

result = (
    totals
    .select("year_start", "week_start", "budget_tier", "l5_avg_salary")
    .orderBy(F.col("budget_tier").desc_nulls_last())
)
