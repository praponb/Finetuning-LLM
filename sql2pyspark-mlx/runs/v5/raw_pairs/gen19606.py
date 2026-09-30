from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "employee_level").orderBy("employee_hire_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("employee_level", "dept_name").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("employee_level", "location").orderBy("last3_avg_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("last3_min_budget", "id")
w6 = Window.partitionBy("dept_name")

valid = (
    df_departments
    .filter(~F.col("location").eqNullSafe(F.lit("Tokyo")))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(", "), "location"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("N/A")))
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.level").alias("employee_level"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

metrics = (
    latest_match
    .filter(F.col("employee_level").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_location",
        "resolved_dept_name",
        "employee_level",
        F.max("budget").over(w2).alias("last3_max_budget"),
        F.avg("budget").over(w2).alias("last3_avg_budget"),
        F.min("budget").over(w2).alias("last3_min_budget"),
        F.sum("budget").over(w3).alias("running_budget"),
    )
)

with_windows = (
    metrics
    .withColumn("running_min_last3_avg_budget", F.min("last3_avg_budget").over(w4))
    .withColumn("running_max_last3_avg_budget", F.max("last3_avg_budget").over(w4))
    .withColumn("running_count_last3_avg_budget", F.count("last3_avg_budget").over(w4))
)

scored = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_location",
        "resolved_dept_name",
        "last3_avg_budget",
        "last3_min_budget",
        "running_budget",
        "running_min_last3_avg_budget",
        "running_max_last3_avg_budget",
        "running_count_last3_avg_budget",
        F.lead("last3_avg_budget").over(w5).alias("next_last3_avg_budget"),
        F.lag("last3_avg_budget").over(w5).alias("prev_last3_avg_budget"),
        F.lead("running_min_last3_avg_budget").over(w5).alias("next_running_min_last3_avg_budget"),
        F.lag("running_min_last3_avg_budget").over(w5).alias("prev_running_min_last3_avg_budget"),
        F.row_number().over(w5).alias("seq_no"),
        F.round((F.col("running_budget") / F.sum("running_budget").over(w6)), 4).alias("running_budget_share"),
    )
)

result = (
    scored
    .select(
        "id",
        "dept_name",
        "prev_last3_avg_budget",
        F.col("prev_running_min_last3_avg_budget").alias("PREV_RUNNING_MIN_LAST3_AVG_BUDGET_VAL"),
        "next_running_min_last3_avg_budget",
    )
    .unpivot(
        ids=["id", "dept_name"],
        values=["prev_last3_avg_budget", "PREV_RUNNING_MIN_LAST3_AVG_BUDGET_VAL", "next_running_min_last3_avg_budget"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
