from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("level").orderBy(F.col("salary_bucket").desc())
w4 = Window.partitionBy("manager_id").orderBy("salary_bucket", "id")
w5 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-25").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.coalesce("full_name", F.lit("N/A")).alias("full_name_filled"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.avg("salary").over(w_running).alias("salary_avg_2"),
        F.ntile(5).over(w2).alias("salary_bucket"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .filter(F.year("hire_date") >= 2023)
    .select(
        "*",
        F.lag("salary", 1).over(w4).alias("prev_salary"),
        F.first("salary_bucket", ignorenulls=True).over(w5).alias("first_known_salary_bucket"),
    )
)

result = (
    scored
    .groupBy("dept_id")
    .pivot("job_title", ["Designer", "Engineer"])
    .agg(F.sum("salary"))
)
