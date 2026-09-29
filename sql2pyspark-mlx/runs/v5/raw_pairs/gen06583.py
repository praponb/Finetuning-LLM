from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy("location", "dept_name").orderBy(F.col("prev_budget").desc(), "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").eqNullSafe(F.lit("Finance")))
    .select("*", F.initcap(F.trim("location")).alias("location_title"))
)

metrics = (
    cleaned
    .withColumn("prev_budget", F.lag("budget").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("budget_dense_rank", F.dense_rank().over(w2))
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary").between(123000, 141000)), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isin([4, 24])), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_name", "location", "location_title", "prev_budget", "event_seq")
)

filtered = (
    deduped
    .filter(~F.col("location").isin(["Singapore", "Berlin", "Bangkok"]))
    .select(
        "*",
        F.round((F.col("prev_budget") / F.col("event_seq")), 2).alias("prev_budget_per_event_seq"),
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
    )
)

result = (
    filtered
    .select(
        "id",
        "location",
        "prev_budget",
        "event_seq",
        "prev_budget_per_event_seq",
        "dept_name_location",
        F.when(F.col("location").rlike("^(New York|Chiang Mai)$"), "PRIORITY").otherwise("CORE").alias("location_zone"),
    )
)
