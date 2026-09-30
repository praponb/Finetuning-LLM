from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w_running = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_int", F.col("budget").cast("int"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("DIRECT")))
)

with_windows = (
    base
    .filter(F.col("budget_int") > 46)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "resolved_dept_name",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.last("budget", ignorenulls=True).over(w_running).alias("last_known_budget"),
    )
)

filtered = (
    with_windows
    .filter(F.col("location").isin(["New York", "London", "Tokyo"]))
    .select(
        "*",
        F.concat("location", F.lit(", "), "dept_name").alias("location_dept_name"),
        (F.col("budget") + F.col("last_known_budget")).alias("budget_plus_last_known_budget"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.month(F.col("x.hire_date")).isin([5, 7, 10])), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "dept_name",
        F.col("budget_plus_last_known_budget").alias("BUDGET_PLUS_LAST_KNOWN_BUDGET"),
        "budget_share",
        F.col("budget").alias("BUDGET"),
        F.col("last_known_budget").alias("LAST_KNOWN_BUDGET_VAL"),
    )
    .unpivot(
        ids=["id", "dept_name"],
        values=["BUDGET_PLUS_LAST_KNOWN_BUDGET", "budget_share", "BUDGET", "LAST_KNOWN_BUDGET_VAL"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
