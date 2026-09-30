from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w_recent = Window.partitionBy("job_title", "level").orderBy("latest_rank", "id")
w3 = Window.orderBy(F.col("salary").desc())
w4 = Window.orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title")
w7 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter(F.col("hire_date") > "2023-03-01")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .withColumn("next_salary", F.lead("salary", 1).over(w_recent))
    .withColumn("salary_rank", F.rank().over(w3))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.dept_name"), F.col("b.budget"), F.col("b.location"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.ntile(4).over(w4).alias("salary_bucket"),
        F.first("budget", ignorenulls=True).over(w5).alias("first_known_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w6)), 4).alias("budget_share"),
        F.sum("budget").over(w7).alias("running_budget"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "location",
        "next_salary",
        F.col("budget_share").alias("BUDGET_SHARE_VAL"),
    )
    .unpivot(["id", "location"], ["next_salary", "BUDGET_SHARE_VAL"], "measure", "val")
    .filter(F.col("val").isNotNull())
)
