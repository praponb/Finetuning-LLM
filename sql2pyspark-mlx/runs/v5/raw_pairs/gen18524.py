from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-3, -1)
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.substring("location", 1, 4).alias("location_prefix"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

scored = (
    valid
    .select(
        "id",
        "budget",
        F.avg("budget").over(w_ordered).alias("budget_avg_3_prior"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.hire_date"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") >= F.col("s.hire_date")), "left_anti")
)

result = (
    kept
    .select("id", "budget", "row_seq", "job_title", "hire_date")
    .orderBy(F.col("hire_date").desc())
)
