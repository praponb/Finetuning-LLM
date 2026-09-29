from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("days_since_hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy("days_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy(F.col("budget").desc_nulls_first())

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-02-21").cast("date")) & F.col("manager_id").isNull())
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
)

metrics = (
    cleaned
    .filter(F.lower(F.trim("full_name")) == "mia rossi")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "days_since_hire_date",
        F.row_number().over(w).alias("row_seq"),
        F.sum("days_since_hire_date").over(w2).alias("running_days_since_hire_date"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.email",
        "s.dept_id",
        "s.job_title",
        "s.row_seq",
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "email", "dept_id", "dept_name", "budget")
)

filtered = (
    deduped
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "dept_name",
        "budget",
        F.ceil("budget").alias("budget_whole"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

result = (
    filtered
    .select(
        "id",
        "dept_id",
        "dept_name",
        "budget",
        "budget_whole",
        "resolved_dept_name",
    )
    .orderBy(F.col("dept_name").desc())
)
