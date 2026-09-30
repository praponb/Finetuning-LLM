from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget")
w3 = Window.partitionBy("dept_name").orderBy("prev_budget", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("dept_name")
w5 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("dept_name").orderBy("prev_budget", "id")

with_windows = (
    df_departments
    .select(
        "*",
        F.lag("budget").over(w_grp).alias("prev_budget"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.cume_dist().over(w2).alias("budget_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "prev_budget",
        "seq_no",
        F.avg("budget").over(w3).alias("budget_avg_3_prior"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
        F.first("dept_name", ignorenulls=True).over(w5).alias("first_known_dept_name"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.prev_budget",
        "s.first_known_dept_name",
        F.col("c.level"),
        F.col("c.salary").alias("employee_salary"),
    )
)

scored = (
    latest_match
    .filter(~F.col("level").isin(["L5", "L4"]))
    .withColumn("dept_name_avg_prev_budget", F.avg("prev_budget").over(w4))
    .withColumn("row_seq", F.row_number().over(w7))
)

result = (
    scored
    .select(
        "id",
        "dept_name",
        F.col("employee_salary").alias("EMPLOYEE_SALARY"),
        F.col("prev_budget").alias("PREV_BUDGET"),
    )
    .unpivot(
        ids=["id", "dept_name"],
        values=["EMPLOYEE_SALARY", "PREV_BUDGET"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
