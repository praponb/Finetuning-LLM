from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Chiang Mai"))))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("NONE")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .filter(F.col("rank_in_group") < 1)
    .select(
        "id",
        "budget",
        "rank_in_group",
        F.percent_rank().over(w).alias("budget_percent_rank"),
        F.rank().over(w).alias("budget_rank"),
        F.ntile(4).over(w2).alias("budget_bucket"),
        F.first("location", ignorenulls=True).over(w3).alias("first_known_location"),
        F.sum("budget").over(w4).alias("cumulative_budget"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"), F.col("c.salary"))
)

kept_keys = (
    latest_match
    .select("id")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name").isin(["Engineering", "Finance"]))
        .select("id")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.employee_hire_date")), "left_anti")
)

result = (
    kept
    .select(
        "budget_rank",
        "budget_bucket",
        "first_known_location",
        "employee_hire_date",
    )
    .orderBy(F.col("employee_hire_date").desc())
)
