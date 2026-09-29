from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("adjusted_budget", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("location").orderBy("adjusted_budget", "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.round((F.col("budget") * 1.15), 2).alias("adjusted_budget"))
)

scored = (
    valid
    .select(
        "*",
        F.sum("adjusted_budget").over(w_hist).alias("cumulative_adjusted_budget"),
        F.ntile(5).over(w2).alias("adjusted_budget_bucket"),
    )
)

base = (
    scored
    .filter(F.col("adjusted_budget_bucket") != 6)
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

with_last = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

deduped = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.is_remote")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "adjusted_budget",
        "cumulative_adjusted_budget",
        "adjusted_budget_bucket",
        "budget_filled",
        "dept_name_filled",
        F.when(F.col("location").rlike("^(New York|London)$"), "GROUP_B").otherwise("EXTENDED").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").desc())
)
