from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("rn").desc(), "id")
w3 = Window.partitionBy("dept_name", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id", "dept_name").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("salary", "id")
w6 = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_name").orderBy("salary")
w8 = Window.partitionBy("manager_id").orderBy("salary_adjusted", "id")

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-07-08").cast("date"))
    .withColumn("salary_adjusted", F.round((F.col("salary") * 0.9), 2))
    .withColumn("resolved_level", F.coalesce("level", F.lit("DIRECT")))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.budget"), F.col("d.dept_name"))
)

with_windows = (
    joined
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.sum("salary").over(w3).alias("running_salary"),
        F.last("salary_adjusted", ignorenulls=True).over(w4).alias("last_known_salary_adjusted"),
        F.lag("budget", 1).over(w5).alias("prev_budget"),
    )
)

enriched = (
    with_windows
    .filter(F.col("manager_id").isin([21, 25, 28]))
    .withColumn("running_prev_budget", F.sum("prev_budget").over(w6))
    .withColumn("salary_rank", F.rank().over(w7))
    .withColumn("prev_budget_2", F.lag("budget", 1).over(w8))
)

stats = (
    enriched
    .rollup(
        "dept_name",
        F.when(F.col("prev_budget") >= 3730000, "HIGH").otherwise("LOW").alias("prev_budget_tier"),
    )
    .agg(
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_rows"),
        F.count_if(F.col("level") == "L2").alias("l2_events"),
    )
)

result = (
    stats
    .select("prev_budget_tier", "engineering_events", "l2_events")
    .orderBy(F.col("prev_budget_tier").desc(), "engineering_events")
)
