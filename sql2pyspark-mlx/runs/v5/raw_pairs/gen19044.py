from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("location").isNull())
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

with_latest = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

metrics = (
    with_latest
    .select(
        "id",
        "dept_name",
        "budget",
        "location_filled",
        F.max("budget").over(w2).alias("last3_max_budget"),
        F.count("budget").over(w2).alias("last3_count_budget"),
        F.avg("budget").over(w2).alias("last3_avg_budget"),
    )
)

base = (
    metrics
    .filter(F.col("dept_name").isNotNull() & F.col("last3_avg_budget").between(1135000, 1396000))
    .select(
        "id",
        "dept_name",
        "budget",
        "location_filled",
        "last3_count_budget",
        "last3_avg_budget",
        F.coalesce("last3_count_budget", F.lit(0)).alias("last3_count_budget_filled"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

result = (
    base
    .select(
        "id",
        "dept_name",
        F.col("last3_avg_budget").alias("LAST3_AVG_BUDGET"),
        F.col("budget").alias("BUDGET_VAL"),
        "last3_count_budget_filled",
    )
    .unpivot(
        ids=["id", "dept_name"],
        values=["LAST3_AVG_BUDGET", "BUDGET_VAL", "last3_count_budget_filled"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
