from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("job_title", "level").orderBy("budget_rounded", "id")
w3 = Window.partitionBy("level", "dept_name").orderBy("dedup_rank", "id")
w4 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("resolved_location", F.coalesce("location", F.lit("OTHER")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isNotNull()), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.level"))
)

deduped = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    deduped
    .filter(F.col("location").isin(["London", "Tokyo"]))
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("location_prefix", F.substring("location", 1, 3))
    .withColumn("resolved_location_2", F.coalesce("location", F.lit("N/A")))
)

result = (
    base
    .withColumn("prev_budget", F.lag("budget").over(w3))
    .withColumn("next_budget_rounded", F.lead("budget_rounded").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("budget_avg_6", F.avg("budget").over(w4))
)
