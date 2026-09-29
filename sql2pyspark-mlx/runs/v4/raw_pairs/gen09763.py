from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level")
w5 = Window.orderBy("hire_date", "id")
w_hist = Window.orderBy(F.col("hire_date").desc(), "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select("*", F.lower("email").alias("email_lower"))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
    .withColumn("cumulative_salary", F.sum("salary").over(w_ordered))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w4)), 4))
)

scored_employees = (
    scored
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("seq_no_2", F.row_number().over(w_hist))
)

filtered = (
    scored_employees
    .filter((F.col("hire_date") >= F.lit("2024-02-20").cast("date")) & (F.col("cumulative_salary") <= 522456))
    .select(
        "*",
        F.upper("full_name").alias("full_name_upper"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

result = (
    filtered
    .withColumn("salary_tier", F.when(F.col("salary") >= 158000, "HIGH").otherwise("LOW"))
    .cube("level", "salary_tier")
    .agg(
        F.percentile_approx("seq_no_2", 0.95).alias("seq_no_2_p95"),
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("salary_tier")
)
