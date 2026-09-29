from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_whole", "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") != "HR"))
    .withColumn("budget_whole", F.ceil("budget"))
    .withColumn("location_filled", F.coalesce("location", F.lit("NONE")))
)

scored = (
    cleaned
    .select(
        "id",
        "dept_name",
        F.ntile(10).over(w).alias("budget_whole_bucket"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.job_title").isin(["Manager", "Intern"])), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id") == 30), "left_anti")
)

result = (
    kept
    .select("id", "budget_whole_bucket", "budget_share", "event_seq")
    .orderBy("event_seq", "budget_whole_bucket")
)
