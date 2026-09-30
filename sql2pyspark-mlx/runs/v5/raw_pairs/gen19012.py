from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("location").orderBy("budget", "id")
w_recent = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w4 = Window.partitionBy("location")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("budget") < 3153000)
    .select("*", F.lower("dept_name").alias("dept_name_lower"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").isNull()), "left_anti")
)

scored = (
    kept
    .select(
        "id",
        "dept_name",
        "budget",
        F.lead("budget", 1).over(w2).alias("next_budget"),
        F.cume_dist().over(w_recent).alias("budget_cume_dist"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

result = (
    with_last
    .select(
        "id",
        "dept_name",
        "budget",
        "next_budget",
        "budget_cume_dist",
        "job_title",
        "employee_hire_date",
        F.when(F.col("dept_name").rlike("^(Sales|HR)$"), "GROUP_B").otherwise("PRIORITY").alias("dept_name_zone"),
    )
)
