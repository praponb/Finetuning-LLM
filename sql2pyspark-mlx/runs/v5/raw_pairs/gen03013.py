from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")
w_ordered = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("dept_id") == 38) & (F.col("hire_date") >= F.lit("2023-02-08").cast("date")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.substring("level", 1, 2).alias("level_prefix"),
    )
)

scored = (
    cleaned
    .filter(F.col("full_name").isNotNull())
    .select(
        "*",
        F.lag("salary", 2).over(w).alias("prev_salary"),
        F.sum("salary").over(w2).alias("running_salary"),
        F.ntile(10).over(w3).alias("salary_bucket"),
        F.sum("salary").over(w_ordered).alias("cumulative_salary"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "level",
        "is_remote_label",
        "prev_salary",
        "running_salary",
        "cumulative_salary",
    )
)

matched_keys = (
    ranked
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("job_title").isin(["Intern", "Analyst", "Director"]))
        .select("job_title")
    )
)

verified = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "email",
        "job_title",
        "salary",
        "level",
        "is_remote_label",
        "prev_salary",
        "running_salary",
        F.when(F.col("job_title").rlike("^(Manager|Intern)$"), "GROUP_B").when(F.col("job_title").rlike("^(Engineer|Analyst)$"), "CORE").otherwise("PRIORITY").alias("job_title_zone"),
    )
)
