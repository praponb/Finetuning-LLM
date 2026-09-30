from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.orderBy("dedup_rank", "id")
w3 = Window.partitionBy("level").orderBy(F.col("l4_rows").desc())

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-14").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "level",
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location") != "Chiang Mai")
    .select(
        "src.id",
        "src.full_name",
        "src.email",
        "src.dept_id",
        "src.hire_date",
        "src.level",
        F.col("d.dept_name"),
    )
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = ranked.select("*", F.row_number().over(w2).alias("event_seq"))

joined = (
    scored
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.dept_name").alias("department_dept_name"))
)

result = (
    joined
    .groupBy(
        "level",
        "dept_id",
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("department_dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("event_seq_per_marketing"),
    )
    .withColumn("l4_rows_rank", F.rank().over(w3))
    .orderBy(F.col("l4_rows").desc())
)
