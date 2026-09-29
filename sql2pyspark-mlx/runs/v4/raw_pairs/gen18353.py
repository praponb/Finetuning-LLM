from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary_adjusted").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2024-01-31").cast("date")) & F.col("level").isNotNull())
    .withColumn("salary_adjusted", F.round((F.col("salary") * 0.9), 2))
    .withColumn("clean_level", F.trim("level"))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

metrics = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cumulative_salary_adjusted", F.sum("salary_adjusted").over(w2))
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept_keys = (
    ranked
    .select("dept_id")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isNull())
        .select("dept_id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-09-21"))) & (F.col("salary") > 132000))
    .withColumn("level_lower", F.lower("level"))
    .withColumn("event_seq_whole", F.ceil("event_seq"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("N/A")))
)

result = (
    valid
    .select("level", "salary", "salary_adjusted")
    .unpivot(["level"], ["salary", "salary_adjusted"], "metric_name", "value")
)
