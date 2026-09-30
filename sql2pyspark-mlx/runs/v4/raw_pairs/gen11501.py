from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("dept_name")
w_hist = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w5 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget_int").desc_nulls_first(), "id")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_int", F.col("budget").cast("int"))
)

scored = (
    base
    .select(
        "*",
        F.lag("budget", 1).over(w_grp).alias("prev_budget"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.min("prev_budget").over(w_running).alias("dept_name_min_prev_budget"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    ranked
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["London", "Chiang Mai"]))
        .select("location", "dept_name")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level").isin(["L3", "L5"])), "left_anti")
)

base_departments = (
    kept
    .filter(F.col("budget").between(2081000, 4018000))
    .select(
        "dept_name",
        "location",
        "budget_int",
        "prev_budget",
        "running_budget",
        "event_seq",
        "rn",
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

result = (
    base_departments
    .select(
        "dept_name",
        "budget_int",
        "prev_budget",
        "running_budget",
        "event_seq",
        "rn",
        "dept_name_upper",
        "resolved_dept_name",
    )
    .orderBy(F.col("budget_int").desc_nulls_last(), F.col("prev_budget").desc())
)
