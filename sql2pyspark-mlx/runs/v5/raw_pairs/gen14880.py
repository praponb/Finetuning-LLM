from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("employee_salary", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_first(), "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        F.col("c.salary").alias("employee_salary"),
    )
)

scored = (
    with_last
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w2))
    .withColumn("budget_avg_5_prior", F.avg("budget").over(w3))
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "employee_salary",
        "first_known_budget",
        "budget_avg_5_prior",
    )
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("dept_name")
    .agg(
        F.sum("first_known_budget").alias("total_first_known_budget"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.max("budget_avg_5_prior").alias("max_budget_avg_5_prior"),
    )
    .orderBy(F.col("total_first_known_budget").asc_nulls_last())
)
