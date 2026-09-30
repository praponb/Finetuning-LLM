from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name", "level").orderBy(F.col("budget").desc_nulls_last(), "id")
w3 = Window.partitionBy("dept_name").orderBy("dedup_rank", "id")

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        F.col("c.salary").alias("employee_salary"),
        F.col("c.level"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").isNotNull()), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("dept_name").isNotNull())
    .withColumn("employee_salary_whole", F.ceil("employee_salary"))
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy("level")
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_rows"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc())
)
