from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w3 = Window.partitionBy("location").orderBy(F.col("budget_percent_rank").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Marketing"]))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("unassigned")))
)

scored = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget_percent_rank",
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

result = (
    with_last
    .select("id")
    .orderBy(F.col("id").desc())
)
