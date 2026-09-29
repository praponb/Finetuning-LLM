from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("hire_date_minus_21d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-06-20").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lower("email").alias("email_lower"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

cleaned = (
    base
    .filter(F.col("level").isNotNull() & F.col("dept_id").isin([8, 15, 20]))
    .select("*", F.date_sub("hire_date", 21).alias("hire_date_minus_21d"))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.budget"))
)

scored = (
    with_dims
    .select(
        "*",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.location"), F.col("d.budget").alias("department_budget"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("salary") >= 148000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_budget", 0.9).alias("cumulative_budget_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("level") == "L5").alias("l5_events"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
