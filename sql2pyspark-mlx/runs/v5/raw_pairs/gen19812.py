from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy("salary", "id")
w_ordered = Window.partitionBy("manager_id").orderBy("prev_salary", "id")
w4 = Window.partitionBy("location").orderBy("next_salary")
w_part = Window.partitionBy("manager_id").orderBy("prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_id", "location").orderBy("next_event_seq", "id")

cleaned = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L5"))) & (F.col("hire_date") <= "2024-04-01"))
    .select("*", F.substring("email", 1, 3).alias("email_prefix"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.manager_id",
        "s.job_title",
        "s.salary",
        "s.is_remote",
        F.col("p.location"),
    )
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    latest
    .select(
        "id",
        "dept_id",
        "manager_id",
        "is_remote",
        "location",
        F.lag("salary").over(w2).alias("prev_salary"),
        F.lead("salary").over(w2).alias("next_salary"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("event_seq", 1).over(w_ordered).alias("next_event_seq"),
        F.rank().over(w4).alias("next_salary_rank"),
        F.sum("next_salary").over(w_part).alias("cumulative_next_salary"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.location").alias("department_location"),
        F.col("e.location").alias("employee_location"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.event_seq")).alias("max_event_seq"),
    )
    .orderBy(F.col("max_event_seq").asc_nulls_first())
)
